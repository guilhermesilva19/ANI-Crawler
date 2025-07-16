import { NextResponse } from 'next/server';
import { getDatabase } from '@/lib/mongodb';

// Helper function to categorize deleted pages by URL patterns
function categorizeDeletedPages(deletedPages: any[]): {
  total: number;
  pdfs: number;
  documents: number;
  webpages: number;
  media: number;
  archives: number;
} {
  const breakdown = {
    total: deletedPages.length,
    pdfs: 0,
    documents: 0,
    webpages: 0,
    media: 0,
    archives: 0
  };
  
  deletedPages.forEach(page => {
    const url = page.url?.toLowerCase() || '';
    
    // PDF files
    if (url.includes('.pdf') || url.includes('/pdf')) {
      breakdown.pdfs++;
    }
    // Office documents
    else if (url.match(/\.(doc|docx|xls|xlsx|ppt|pptx|txt|rtf)/i) || 
             url.includes('/download') || url.includes('/files') || url.includes('/attachments')) {
      breakdown.documents++;
    }
    // Media files
    else if (url.match(/\.(jpg|jpeg|png|gif|bmp|svg|webp|mp4|avi|mov|mp3|wav)/i)) {
      breakdown.media++;
    }
    // Archive files
    else if (url.match(/\.(zip|rar|7z|tar|gz)/i)) {
      breakdown.archives++;
    }
    // Default to webpage
    else {
      breakdown.webpages++;
    }
  });
  
  return breakdown;
}

export async function GET(
  request: Request,
  { params }: { params: Promise<{ siteId: string }> }
) {
  try {
    const { siteId } = await params;
    
    // Convert site ID format (dashboard uses hyphens, database uses underscores)
    const dbSiteId = siteId.replace(/-/g, '_');
    
    const db = await getDatabase();
    
    // Calculate UTC boundaries for "last 7 days" (7-day rolling window)
    const utcNow = new Date();
    const utc7DaysAgo = new Date(utcNow);
    utc7DaysAgo.setDate(utc7DaysAgo.getDate() - 6); // 6 days ago + today = 7 days
    utc7DaysAgo.setUTCHours(0, 0, 0, 0);
    const utcEndOfToday = new Date(utcNow);
    utcEndOfToday.setUTCHours(23, 59, 59, 999);
    
    // Get 7-day date range in AEST format for daily_stats collection queries
    // Daily stats uses AEST date strings, calculate the date range
    const aestOffset = 10 * 60 * 60 * 1000; // AEST is UTC+10 in milliseconds
    const aestDates = [];
    for (let i = 6; i >= 0; i--) {
      const utcDate = new Date(utcNow.getTime() - (i * 24 * 60 * 60 * 1000));
      const aestDate = new Date(utcDate.getTime() + aestOffset);
      aestDates.push(aestDate.toISOString().split('T')[0]);
    }
    
    // 1. Get processing activity from daily_stats (aggregated over 7 days)
    const dailyStatsDocs = await db.collection('daily_stats').find({
      site_id: dbSiteId,
      date: { $in: aestDates }
    }).toArray();
    
    // Aggregate processing stats
    const processingStats = {
      pages_crawled: 0,
      new_pages: 0,
      changed_pages: 0,
      failed_pages: 0,
      deleted_pages: 0,
      document_pages: 0
    };
    
    const dailyBreakdown = [];
    
    for (const dateStr of aestDates) {
      const dayStats = dailyStatsDocs.find(doc => doc.date === dateStr)?.stats || {
        pages_crawled: 0,
        new_pages: 0,
        changed_pages: 0,
        failed_pages: 0,
        deleted_pages: 0,
        document_pages: 0
      };
      
      // Add to totals
      processingStats.pages_crawled += dayStats.pages_crawled;
      processingStats.new_pages += dayStats.new_pages;
      processingStats.changed_pages += dayStats.changed_pages;
      processingStats.failed_pages += dayStats.failed_pages;
      processingStats.deleted_pages += dayStats.deleted_pages;
      processingStats.document_pages += dayStats.document_pages;
      
      // Store daily breakdown
      dailyBreakdown.push({
        date: dateStr,
        stats: dayStats
      });
    }
    
    // 2. Get pages discovered in last 7 days (first_seen in date range)
    const discoveredThisWeek = await db.collection('url_states').countDocuments({
      site_id: dbSiteId,
      first_seen: {
        $gte: utc7DaysAgo,
        $lte: utcEndOfToday
      }
    });
    
    // 3. Get pages that changed in last 7 days (last_change.timestamp in date range)
    const changedThisWeek = await db.collection('url_states').countDocuments({
      site_id: dbSiteId,
      'last_change.timestamp': {
        $gte: utc7DaysAgo,
        $lte: utcEndOfToday
      }
    });
    
    // 4. Get pages that failed in last 7 days (using performance_history for precise timing)
    const failedThisWeek = await db.collection('performance_history').countDocuments({
      site_id: dbSiteId,
      page_type: 'failed',
      timestamp: {
        $gte: utc7DaysAgo,
        $lte: utcEndOfToday
      }
    });
    
    // 5. Get pages that were deleted in last 7 days with breakdown by type
    const deletedDocsThisWeek = await db.collection('performance_history').find({
      site_id: dbSiteId,
      page_type: 'deleted',
      timestamp: {
        $gte: utc7DaysAgo,
        $lte: utcEndOfToday
      }
    }).toArray();
    
    // Categorize deleted pages by URL patterns
    const deletedBreakdown = categorizeDeletedPages(deletedDocsThisWeek);
    
    // 6. Get current status totals for context
    const [totalPages, visitedPages, remainingPages, failedPages, deletedPages] = await Promise.all([
      db.collection('url_states').countDocuments({ site_id: dbSiteId }),
      db.collection('url_states').countDocuments({ site_id: dbSiteId, status: 'visited' }),
      db.collection('url_states').countDocuments({ site_id: dbSiteId, status: 'remaining' }),
      db.collection('url_states').countDocuments({ 
        site_id: dbSiteId, 
        'status_info.status': { $gte: 400 } 
      }),
      db.collection('url_states').countDocuments({ 
        site_id: dbSiteId, 
        'status_info.status': { $in: [404, 410] },
        'status_info.error_count': { $gte: 2 }
      })
    ]);
    
    // 7. Calculate daily averages
    const dailyAverages = {
      discovered_per_day: Math.round(discoveredThisWeek / 7),
      crawled_per_day: Math.round(processingStats.pages_crawled / 7),
      changed_per_day: Math.round(changedThisWeek / 7),
      failed_per_day: Math.round(failedThisWeek / 7),
      deleted_per_day: Math.round(deletedBreakdown.total / 7)
    };
    
    const response = {
      timeframe: 'weekly',
      period: `Last 7 days UTC`,
      timezone: 'UTC',
      discovery: {
        discovered_this_week: discoveredThisWeek,
        daily_average: dailyAverages.discovered_per_day,
        description: 'Pages discovered (first_seen) in last 7 days UTC'
      },
      processing: {
        crawled_this_week: processingStats.pages_crawled,
        daily_average: dailyAverages.crawled_per_day,
        new_processed: processingStats.new_pages,
        changed_processed: processingStats.changed_pages,
        failed_processed: processingStats.failed_pages,
        deleted_processed: processingStats.deleted_pages,
        document_processed: processingStats.document_pages,
        description: 'Pages processed by crawler in last 7 days UTC'
      },
      changes: {
        changed_this_week: changedThisWeek,
        daily_average: dailyAverages.changed_per_day,
        description: 'Pages with content changes detected in last 7 days UTC'
      },
      failures: {
        failed_this_week: failedThisWeek,
        daily_average: dailyAverages.failed_per_day,
        description: 'Pages that failed to load in last 7 days UTC'
      },
      deletions: {
        deleted_this_week: deletedBreakdown.total,
        deleted_pdfs: deletedBreakdown.pdfs,
        deleted_documents: deletedBreakdown.documents,
        deleted_webpages: deletedBreakdown.webpages,
        deleted_media: deletedBreakdown.media,
        deleted_archives: deletedBreakdown.archives,
        daily_average: dailyAverages.deleted_per_day,
        description: 'Pages detected as deleted in last 7 days UTC with breakdown by type'
      },
      daily_breakdown: dailyBreakdown,
      current_totals: {
        total_pages: totalPages,
        visited_pages: visitedPages,
        remaining_pages: remainingPages,
        failed_pages: failedPages,
        deleted_pages: deletedPages,
        description: 'Current cumulative totals for context'
      },
      metadata: {
        utc_start: utc7DaysAgo.toISOString(),
        utc_end: utcEndOfToday.toISOString(),
        aest_date_range: aestDates,
        query_time: new Date().toISOString(),
        note: 'Using UTC for all calculations and display'
      }
    };
    
    return NextResponse.json(response);
  } catch (error) {
    console.error('Error fetching weekly stats:', error);
    return NextResponse.json(
      { error: 'Failed to fetch weekly stats' },
      { status: 500 }
    );
  }
} 