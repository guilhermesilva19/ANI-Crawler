import { NextResponse } from 'next/server';
import { getSiteStates, getUrlStates, getDailyStats, getPerformanceHistory } from '@/lib/mongodb';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ siteId: string }> }
) {
  try {
    const { siteId } = await params;
    
    // Get site state
    const siteStates = await getSiteStates();
    const siteState = await siteStates.findOne({ site_id: siteId });
    
    if (!siteState) {
      return NextResponse.json(
        { error: 'Site not found' },
        { status: 404 }
      );
    }
    
    // Get URL counts
    const urlStates = await getUrlStates();
    const visitedCount = await urlStates.countDocuments({ 
      site_id: siteId, 
      status: 'visited' 
    });
    const remainingCount = await urlStates.countDocuments({ 
      site_id: siteId, 
      status: 'remaining' 
    });
    
    // Get today's stats
    const today = new Date().toISOString().split('T')[0];
    const dailyStats = await getDailyStats();
    const todayStats = await dailyStats.findOne({ 
      site_id: siteId, 
      date: today 
    });
    
    // Get recent performance to check for actual crawler activity
    const perfHistory = await getPerformanceHistory();
    const recentPerf = await perfHistory.find({ 
      site_id: siteId 
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
    let avgCrawlTimeSeconds = 0;
    if (recentPerf.length > 0) {
      const totalCrawlTime = recentPerf.reduce((sum: number, p: any) => sum + (p.crawl_time || 0), 0);
      avgCrawlTimeSeconds = totalCrawlTime / recentPerf.length;
      currentSpeed = avgCrawlTimeSeconds > 0 ? Math.round(3600 / avgCrawlTimeSeconds) : 0;
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
      avgCrawlTime: avgCrawlTimeSeconds > 0 ? avgCrawlTimeSeconds.toFixed(1) : '0.0',
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
      }
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