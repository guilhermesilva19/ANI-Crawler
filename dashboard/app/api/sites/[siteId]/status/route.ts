import { NextResponse } from 'next/server';
import { getSiteStates, getUrlStates, getDailyStats, getPerformanceHistory } from '@/lib/mongodb';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ siteId: string }> }
) {
  try {
    const { siteId } = await params;
    
    // Convert site ID format (dashboard uses hyphens, database uses underscores)
    const dbSiteId = siteId.replace(/-/g, '_');
    
    // Get site state
    const siteStates = await getSiteStates();
    const siteState = await siteStates.findOne({ site_id: dbSiteId });
    
    if (!siteState) {
      return NextResponse.json(
        { error: 'Site not found' },
        { status: 404 }
      );
    }
    
    // Get URL counts
    const urlStates = await getUrlStates();
    const visitedCount = await urlStates.countDocuments({ 
      site_id: dbSiteId, 
      status: 'visited' 
    });
    const remainingCount = await urlStates.countDocuments({ 
      site_id: dbSiteId, 
      status: 'remaining' 
    });
    
    // Get actual deleted pages count (same logic as URL Explorer)
    const deletedCount = await urlStates.countDocuments({ 
      site_id: dbSiteId, 
      'status_info.status': { $in: [404, 410] },
      'status_info.error_count': { $gte: 2 }
    });
    
    // Get today's stats - ONLY for the selected site
    const today = new Date().toISOString().split('T')[0];
    const dailyStats = await getDailyStats();
    
    const todayStats = await dailyStats.findOne({ 
      site_id: dbSiteId, 
      date: today 
    });
    
    // Get recent performance to check for actual crawler activity
    const perfHistory = await getPerformanceHistory();
    const recentPerf = await perfHistory.find({ 
      site_id: dbSiteId 
    })
    .sort({ timestamp: -1 })
    .limit(20)
    .toArray();
    
    // Check for actual recent activity within 5 minutes
    const now = Date.now();
    const fiveMinutesMs = 5 * 60 * 1000;
    const actualRecentPerf = recentPerf.filter(perf => {
      const perfTime = new Date(perf.timestamp).getTime();
      const timeDiffMs = Math.abs(now - perfTime);
      return timeDiffMs < fiveMinutesMs && perfTime <= now;
    });
    
    // Only active if there's real activity within last 5 minutes
    const hasRecentActivity = actualRecentPerf.length > 0;
    
    // Calculate REAL total discovered pages (not estimate!)
    const totalDiscoveredPages = visitedCount + remainingCount;
    
    // Calculate current speed - avoid NaN
    let currentSpeed = 0;
    let avgTimeBetweenUrls = 0;
    
    if (recentPerf.length >= 2) {
      // Sort by timestamp to ensure chronological order
      const sortedPerf = recentPerf.sort((a: any, b: any) => 
        new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
      );
      
      // Calculate time differences between consecutive URLs
      let totalTimeBetweenUrls = 0;
      let validIntervals = 0;
      
      for (let i = 1; i < sortedPerf.length; i++) {
        const currentTime = new Date(sortedPerf[i].timestamp).getTime();
        const previousTime = new Date(sortedPerf[i-1].timestamp).getTime();
        const timeDiffSeconds = (currentTime - previousTime) / 1000;
        
        // Only count reasonable intervals (between 10 seconds and 10 minutes)
        if (timeDiffSeconds >= 10 && timeDiffSeconds <= 600) {
          totalTimeBetweenUrls += timeDiffSeconds;
          validIntervals++;
        }
      }
      
      // Calculate average time between URLs and real speed
      avgTimeBetweenUrls = validIntervals > 0 ? totalTimeBetweenUrls / validIntervals : 0;
      currentSpeed = avgTimeBetweenUrls > 0 ? Math.round(3600 / avgTimeBetweenUrls) : 0;
    } else if (recentPerf.length === 1) {
      // Fallback for single record: estimate based on processing time + typical delay
      const crawlTime = recentPerf[0].crawl_time || 30;
      const estimatedTotalTime = crawlTime + 30; // Add 30s delay
      currentSpeed = Math.round(3600 / estimatedTotalTime);
    }
    
    // Calculate progress using DISCOVERED pages, not estimates
    const completedPages = visitedCount;
    const progressPercent = totalDiscoveredPages > 0 
      ? Math.round((completedPages / totalDiscoveredPages) * 100) 
      : 0;
    
    // Calculate ETA with fallback
    let etaHours = null;
    if (remainingCount > 0) {
      if (currentSpeed > 0) {
        etaHours = remainingCount / currentSpeed;
      } else {
        // Fallback: Use estimated 15 seconds per page if no performance data
        const fallbackSpeed = 3600 / 15; // 240 pages/hour
        etaHours = remainingCount / fallbackSpeed;
      }
    }
    
    const status = {
      siteId,
      isActive: hasRecentActivity,  // FIXED: Based on actual activity within 5 minutes
      totalPages: totalDiscoveredPages,
      completedPages,
      remainingPages: remainingCount,
      progressPercent,
      currentSpeed,
      avgCrawlTime: avgTimeBetweenUrls > 0 ? avgTimeBetweenUrls.toFixed(1) : '0.0',
      etaHours,
      cycleInfo: {
        number: siteState.current_cycle || 1,
        type: siteState.is_first_cycle ? 'Discovery' : 'Maintenance',
        startTime: siteState.cycle_start_time,
        durationDays: siteState.cycle_start_time 
          ? Math.floor((Date.now() - new Date(siteState.cycle_start_time).getTime()) / (1000 * 60 * 60 * 24))
          : 0
      },
      todayStats: todayStats?.stats || {
        pages_crawled: 0,
        new_pages: 0,
        changed_pages: 0,
        errors: 0
      },
      deletedCount
    };
    
    return NextResponse.json({ status });
  } catch (error) {
    console.error('Error fetching site status:', error);
    return NextResponse.json(
      { error: 'Failed to fetch site status' },
      { status: 500 }
    );
  }
} 