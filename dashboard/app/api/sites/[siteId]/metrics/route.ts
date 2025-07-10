import { NextResponse } from 'next/server';
import { getDailyStats, getPerformanceHistory, getUrlStates } from '@/lib/mongodb';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ siteId: string }> }
) {
  try {
    const { siteId } = await params;
    
    // Get last 7 days of daily stats
    const dailyStats = await getDailyStats();
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    
    const dailyData = await dailyStats.find({ 
      site_id: siteId,
      date: { $gte: sevenDaysAgo.toISOString().split('T')[0] }
    })
    .sort({ date: 1 })
    .toArray();
    
    // Get last 24 hours of performance data for speed trend
    const perfHistory = await getPerformanceHistory();
    const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
    
    const hourlyPerf = await perfHistory.find({ 
      site_id: siteId,
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
    
    const speedTrend = Object.entries(hourlyGroups).map(([hour, perfs]) => {
      let avgSpeed = 0;
      if (perfs.length > 0) {
        const totalCrawlTime = perfs.reduce((sum: number, p: any) => sum + (p.crawl_time || 0), 0);
        const avgCrawlTime = totalCrawlTime / perfs.length;
        avgSpeed = avgCrawlTime > 0 ? Math.round(3600 / avgCrawlTime) : 0;
      }
      return {
        time: new Date(hour + ':00:00Z').getTime(),
        speed: avgSpeed,
        count: perfs.length
      };
    });
    
    // Get error rate data
    const urlStates = await getUrlStates();
    const errorStats = await urlStates.aggregate([
      { $match: { site_id: siteId } },
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
        avgSpeed: speedTrend.length > 0 
          ? Math.round(speedTrend.reduce((sum: number, s: any) => sum + s.speed, 0) / speedTrend.length)
          : 0,
        peakSpeed: speedTrend.length > 0 
          ? Math.max(...speedTrend.map((s: any) => s.speed))
          : 0,
        minSpeed: speedTrend.length > 0 
          ? Math.min(...speedTrend.map((s: any) => s.speed))
          : 0
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