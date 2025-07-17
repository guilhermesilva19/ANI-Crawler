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
    
    // SINGLE SOURCE OF TRUTH: Count all URL statuses consistently
    const urlStates = await getUrlStates();
    const [visitedCount, remainingCount, inProgressCount, totalDiscoveredCount, deletedCount] = await Promise.all([
      // Completed pages (visited)
      urlStates.countDocuments({ site_id: dbSiteId, status: 'visited' }),
      // Pending pages (remaining to crawl)
      urlStates.countDocuments({ site_id: dbSiteId, status: 'remaining' }),
      // Currently processing pages (in_progress)
      urlStates.countDocuments({ site_id: dbSiteId, status: 'in_progress' }),
      // TOTAL discovered pages (ALL statuses) - Single source of truth
      urlStates.countDocuments({ site_id: dbSiteId }),
      // Deleted pages (failed with multiple errors) - FIXED: Include ALL error codes >= 400
      urlStates.countDocuments({ 
        site_id: dbSiteId, 
        'status_info.status': { $gte: 400 },
        'status_info.error_count': { $gte: 2 }
      })
    ]);
    
    // Get today's stats - ONLY for the selected site
    const today = new Date().toISOString().split('T')[0];
    const dailyStats = await getDailyStats();
    
    const todayStats = await dailyStats.findOne({ 
      site_id: dbSiteId, 
      date: today 
    });
    
    // Get performance history for speed and activity calculations
    const perfHistory = await getPerformanceHistory();
    
    // INTELLIGENT: Use up to 5 hours of data, but adapt to when crawler actually started
    const fiveHoursAgo = new Date(Date.now() - 5 * 60 * 60 * 1000);
    
    // Find when this site's crawling actually started
    const earliestRecord = await perfHistory.findOne(
      { site_id: dbSiteId }, 
      { sort: { timestamp: 1 } }
    );
    
    let speedCalcWindow;
    let hoursOfData;
    
    if (!earliestRecord) {
      // No performance data at all
      speedCalcWindow = fiveHoursAgo;
      hoursOfData = 5;
    } else {
      const crawlerStartTime = new Date(earliestRecord.timestamp);
      const now = new Date();
      const actualHoursRunning = (now.getTime() - crawlerStartTime.getTime()) / (1000 * 60 * 60);
      
      if (actualHoursRunning >= 5) {
        // Crawler has been running 5+ hours, use full 5-hour window
        speedCalcWindow = fiveHoursAgo;
        hoursOfData = 5;
      } else {
        // Crawler started less than 5 hours ago, use all available data
        speedCalcWindow = crawlerStartTime;
        hoursOfData = Math.max(actualHoursRunning, 0.1); // Minimum 6 minutes to avoid division by zero
      }
    }
    
    // Count pages crawled in our intelligent time window
    const pagesInWindow = await perfHistory.countDocuments({ 
      site_id: dbSiteId,
      timestamp: { $gte: speedCalcWindow }
    });
    
    // Calculate speed: pages per hour over the actual time period
    const currentSpeed = Math.round(pagesInWindow / hoursOfData);
    
    // Check for actual recent activity within 10 minutes (more reliable than 5)
    const tenMinutesAgo = new Date(Date.now() - 10 * 60 * 1000);
    const pagesInLast10Min = await perfHistory.countDocuments({ 
      site_id: dbSiteId,
      timestamp: { $gte: tenMinutesAgo }
    });
    
    // Only active if there's real activity within last 10 minutes
    const hasRecentActivity = pagesInLast10Min > 0;
    
    // Calculate progress using TOTAL discovered pages as denominator
    const completedPages = visitedCount;
    const progressPercent = totalDiscoveredCount > 0 
      ? Math.round((completedPages / totalDiscoveredCount) * 100) 
      : 0;
    
    // Calculate ETA based ONLY on remaining + in_progress URLs (all unfinished work)
    const unfinishedPages = remainingCount + inProgressCount;
    let etaHours = null;
    if (unfinishedPages > 0) {
      if (currentSpeed > 0) {
        // ETA = unfinished pages / speed (pages per hour from last 5 hours or since start)
        etaHours = unfinishedPages / currentSpeed;
              } else {
        // Fallback: Use conservative 300 pages/hour if no performance data available
        const fallbackSpeed = 300;
        etaHours = unfinishedPages / fallbackSpeed;
      }
    }
    
    const status = {
      siteId,
      isActive: hasRecentActivity,  // Based on actual activity within 10 minutes
      totalPages: totalDiscoveredCount,  // FIXED: Use single source of truth
      completedPages,
      remainingPages: remainingCount,
      inProgressPages: inProgressCount,  // NEW: Show in_progress status
      unfinishedPages,  // NEW: remaining + in_progress combined
      progressPercent,
      currentSpeed,
      avgCrawlTime: currentSpeed > 0 ? (3600 / currentSpeed).toFixed(1) : '0.0', // Seconds per page based on current speed
      etaHours,
      speedCalculation: {
        hoursOfData: hoursOfData.toFixed(1),
        pagesInWindow,
        windowStart: speedCalcWindow.toISOString(),
        isFullFiveHours: hoursOfData >= 5
      },
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