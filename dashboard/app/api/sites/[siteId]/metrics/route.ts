import { NextResponse } from 'next/server';
import { getDailyStats, getPerformanceHistory, getUrlStates } from '@/lib/mongodb';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ siteId: string }> }
) {
  try {
    const { siteId } = await params;
    
    // Convert site ID format (dashboard uses hyphens, database uses underscores)
    const dbSiteId = siteId.replace(/-/g, '_');
    
    // Get last 7 days of daily stats
    const dailyStats = await getDailyStats();
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    
    const dailyData = await dailyStats.find({ 
      site_id: dbSiteId,
      date: { $gte: sevenDaysAgo.toISOString().split('T')[0] }
    })
    .sort({ date: 1 })
    .toArray();
    
    // Get last 24 hours of performance data for speed trend
    const perfHistory = await getPerformanceHistory();
    const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
    
    // Also get data for intelligent current speed calculation (same logic as status route)
    const fiveHoursAgo = new Date(Date.now() - 5 * 60 * 60 * 1000);
    const earliestRecord = await perfHistory.findOne(
      { site_id: dbSiteId }, 
      { sort: { timestamp: 1 } }
    );
    
    let currentSpeedWindow;
    let currentHoursOfData;
    
    if (!earliestRecord) {
      currentSpeedWindow = fiveHoursAgo;
      currentHoursOfData = 5;
    } else {
      const crawlerStartTime = new Date(earliestRecord.timestamp);
      const now = new Date();
      const actualHoursRunning = (now.getTime() - crawlerStartTime.getTime()) / (1000 * 60 * 60);
      
      if (actualHoursRunning >= 5) {
        currentSpeedWindow = fiveHoursAgo;
        currentHoursOfData = 5;
      } else {
        currentSpeedWindow = crawlerStartTime;
        currentHoursOfData = Math.max(actualHoursRunning, 0.1);
      }
    }
    
    // Get current speed pages for metrics
    const currentSpeedPages = await perfHistory.countDocuments({ 
      site_id: dbSiteId,
      timestamp: { $gte: currentSpeedWindow }
    });
    const currentSpeedCalc = Math.round(currentSpeedPages / currentHoursOfData);
    
    const hourlyPerf = await perfHistory.find({ 
      site_id: dbSiteId,
      timestamp: { $gte: oneDayAgo }
    })
    .sort({ timestamp: 1 })
    .toArray();
    
    // Group performance by hour for chart
    const hourlyGroups: { [key: string]: any[] } = {};
    hourlyPerf.forEach((perf: any) => {
      const hour = new Date(perf.timestamp).toISOString().slice(0, 13);
      if (!hourlyGroups[hour]) hourlyGroups[hour] = [];
      hourlyGroups[hour].push(perf);
    });
    
    // Generate hourly speed trend using simple count-based logic
    const speedTrend = [];
    const hourlyKeys = Object.keys(hourlyGroups).sort();
    
    for (const hour of hourlyKeys) {
      const perfs = hourlyGroups[hour];
      // Simple: Speed = number of pages crawled in that hour
      const hourlySpeed = perfs.length;
      
      speedTrend.push({
        time: new Date(hour + ':00:00Z').getTime(),
        speed: hourlySpeed,
        count: perfs.length
      });
    }
    
    // Get error rate data
    const urlStates = await getUrlStates();
    const errorStats = await urlStates.aggregate([
      { $match: { site_id: dbSiteId } },
      { 
        $group: {
          _id: '$status_info.status_code',
          count: { $sum: 1 }
        }
      }
    ]).toArray();
    
    const totalRequests = errorStats.reduce((sum: number, stat: any) => sum + stat.count, 0);
    const errorCount = errorStats
      .filter((stat: any) => stat._id >= 400)
      .reduce((sum: number, stat: any) => sum + stat.count, 0);
    const errorRate = totalRequests > 0 ? (errorCount / totalRequests) * 100 : 0;
    
    // Discovery trends - pages found over time
    const discoveryTrend = dailyData.map((day: any) => ({
      date: day.date,
      newPages: day.stats?.new_pages || 0,
      totalPages: day.stats?.pages_crawled || 0,
      changedPages: day.stats?.changed_pages || 0
    }));
    
    const metrics = {
      speedTrend,
      dailyStats: discoveryTrend,
      errorRate: Math.round(errorRate * 100) / 100,
      errorCount,
      totalRequests,
      performance: {
        currentSpeed: currentSpeedCalc, // Intelligent 5-hour (or since start) speed calculation
        avgSpeed: speedTrend.length > 0 
          ? Math.round(speedTrend.reduce((sum: number, s: any) => sum + s.speed, 0) / speedTrend.length)
          : 0,
        peakSpeed: speedTrend.length > 0 
          ? Math.max(...speedTrend.map((s: any) => s.speed))
          : 0,
        minSpeed: speedTrend.length > 0 
          ? Math.min(...speedTrend.map((s: any) => s.speed))
          : 0,
        speedCalculation: {
          hoursOfData: currentHoursOfData.toFixed(1),
          pagesInWindow: currentSpeedPages,
          windowStart: currentSpeedWindow.toISOString(),
          isFullFiveHours: currentHoursOfData >= 5
        }
      }
    };
    
    return NextResponse.json({ metrics });
  } catch (error) {
    console.error('Error fetching site metrics:', error);
    return NextResponse.json(
      { error: 'Failed to fetch site metrics' },
      { status: 500 }
    );
  }
} 