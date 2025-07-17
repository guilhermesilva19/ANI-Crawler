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
    
    // Calculate UTC boundaries for "today" (last 24 hours from current UTC time)
    const utcNow = new Date();
    const utcStartOfToday = new Date(utcNow);
    utcStartOfToday.setUTCHours(0, 0, 0, 0);
    const utcEndOfToday = new Date(utcNow);
    utcEndOfToday.setUTCHours(23, 59, 59, 999);
    
    // Get today's date string in AEST format for daily_stats collection query
    // Daily stats uses AEST date strings but we need to calculate which AEST date "today" represents
    const aestOffset = 10 * 60 * 60 * 1000; // AEST is UTC+10 in milliseconds
    const aestNow = new Date(utcNow.getTime() + aestOffset);
    const aestTodayString = aestNow.toISOString().split('T')[0]; // "2025-07-16"
    
    // 1. Get processing activity from daily_stats (pages crawled today)
    const dailyStatsDoc = await db.collection('daily_stats').findOne({
      site_id: dbSiteId,
      date: aestTodayString
    });
    
    const processingStats = dailyStatsDoc?.stats || {
      pages_crawled: 0,
      new_pages: 0,
      changed_pages: 0,
      failed_pages: 0,
      deleted_pages: 0,
      document_pages: 0
    };
    
    // 2. Get pages discovered today (first_seen in last 24 hours AEST)
    const discoveredToday = await db.collection('url_states').countDocuments({
      site_id: dbSiteId,
      first_seen: {
        $gte: utcStartOfToday,
        $lte: utcEndOfToday
      }
    });
    
    // 3. Get pages that changed today (last_change.timestamp in last 24 hours AEST)
    const changedToday = await db.collection('url_states').countDocuments({
      site_id: dbSiteId,
      'last_change.timestamp': {
        $gte: utcStartOfToday,
        $lte: utcEndOfToday
      }
    });
    
    // 4. Get pages that failed today (using performance_history for precise timing)
    const failedToday = await db.collection('performance_history').countDocuments({
      site_id: dbSiteId,
      page_type: 'failed',
      timestamp: {
        $gte: utcStartOfToday,
        $lte: utcEndOfToday
      }
    });
    
    // 5. Get pages that were deleted today with breakdown by type
    const deletedDocsToday = await db.collection('performance_history').find({
      site_id: dbSiteId,
      page_type: 'deleted',
      timestamp: {
        $gte: utcStartOfToday,
        $lte: utcEndOfToday
      }
    }).toArray();
    
    // Categorize deleted pages by URL patterns
    const deletedBreakdown = categorizeDeletedPages(deletedDocsToday);
    
    // 6. Get current status totals for context (CONSISTENT with status route)
    const [totalPages, visitedPages, remainingPages, inProgressPages, failedPages, deletedPages] = await Promise.all([
      db.collection('url_states').countDocuments({ site_id: dbSiteId }),
      db.collection('url_states').countDocuments({ site_id: dbSiteId, status: 'visited' }),
      db.collection('url_states').countDocuments({ site_id: dbSiteId, status: 'remaining' }),
      db.collection('url_states').countDocuments({ site_id: dbSiteId, status: 'in_progress' }),
      db.collection('url_states').countDocuments({ 
        site_id: dbSiteId, 
        'status_info.status': { $gte: 400 } 
      }),
      db.collection('url_states').countDocuments({ 
        site_id: dbSiteId, 
        'status_info.status': { $gte: 400 },
        'status_info.error_count': { $gte: 2 }
      })
    ]);
    
    const response = {
      timeframe: 'daily',
      period: `Last 24 hours UTC (${aestTodayString} AEST date)`,
      timezone: 'UTC',
      discovery: {
        discovered_today: discoveredToday,
        description: 'Pages discovered (first_seen) in last 24 hours UTC'
      },
      processing: {
        crawled_today: processingStats.pages_crawled,
        new_processed: processingStats.new_pages,
        changed_processed: processingStats.changed_pages,
        failed_processed: processingStats.failed_pages,
        deleted_processed: processingStats.deleted_pages,
        document_processed: processingStats.document_pages,
        description: 'Pages processed by crawler today (AEST date boundary)'
      },
      changes: {
        changed_today: changedToday,
        description: 'Pages with content changes detected in last 24 hours UTC'
      },
      failures: {
        failed_today: failedToday,
        description: 'Pages that failed to load in last 24 hours UTC'
      },
      deletions: {
        deleted_today: deletedBreakdown.total,
        deleted_pdfs: deletedBreakdown.pdfs,
        deleted_documents: deletedBreakdown.documents,
        deleted_webpages: deletedBreakdown.webpages,
        deleted_media: deletedBreakdown.media,
        deleted_archives: deletedBreakdown.archives,
        description: 'Pages detected as deleted in last 24 hours UTC with breakdown by type'
      },
      current_totals: {
        total_pages: totalPages,
        visited_pages: visitedPages,
        remaining_pages: remainingPages,
        in_progress_pages: inProgressPages,
        failed_pages: failedPages,
        deleted_pages: deletedPages,
        description: 'Current cumulative totals for context (consistent with status route)'
      },
      metadata: {
        aest_date: aestTodayString,
        utc_start: utcStartOfToday.toISOString(),
        utc_end: utcEndOfToday.toISOString(),
        query_time: new Date().toISOString(),
        note: 'Using UTC for timestamps, AEST date string for daily_stats collection'
      }
    };
    
    return NextResponse.json(response);
  } catch (error) {
    console.error('Error fetching daily stats:', error);
    return NextResponse.json(
      { error: 'Failed to fetch daily stats' },
      { status: 500 }
    );
  }
} 