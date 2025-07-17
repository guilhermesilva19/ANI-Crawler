import { NextResponse } from 'next/server';
import { getUrlStates, getDailyStats } from '@/lib/mongodb';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ siteId: string }> }
) {
  try {
    const { siteId } = await params;
    
    // Convert site ID format (dashboard uses hyphens, database uses underscores)
    const dbSiteId = siteId.replace(/-/g, '_');
    
    // Get URL states to find deleted/failed pages
    const urlStates = await getUrlStates();
    
    // Find pages with error status codes (potential deleted pages)
    const deletedPages = await urlStates.find({
      site_id: dbSiteId,
      'status_info.status': { $gte: 400 }, // All error codes
      'status_info.error_count': { $gte: 2 }      // Multiple failures
    }).toArray();
    
    // Find pages with any errors
    const failedPages = await urlStates.find({
      site_id: dbSiteId,
      'status_info.status': { $gte: 400 }
    }).toArray();
    
    // Get recent page changes
    const changedPages = await urlStates.find({
      site_id: dbSiteId,
      'status_info.last_success': { $exists: true },
      'status_info.status': { $lt: 400 }
    })
    .sort({ 'status_info.last_success': -1 })
    .limit(10)
    .toArray();
    
    // Get last 7 days of daily stats for trends
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    
    const dailyStats = await getDailyStats();
    const weeklyStats = await dailyStats.find({
      site_id: dbSiteId,
      date: { $gte: sevenDaysAgo.toISOString().split('T')[0] }
    })
    .sort({ date: 1 })
    .toArray();
    
    // Calculate health metrics
    const totalPages = await urlStates.countDocuments({ site_id: dbSiteId });
    const healthyPages = totalPages - failedPages.length;
    const healthScore = totalPages > 0 ? (healthyPages / totalPages) * 100 : 100;
    
    const healthData = {
      deletedPages: deletedPages.map((page: any) => ({
        url: page.url,
        status: page.status_info?.status || 404,
        lastSuccess: page.status_info?.last_success,
        errorCount: page.status_info?.error_count || 0
      })),
      failedPages: failedPages.length,
      changedPages: changedPages.map((page: any) => ({
        url: page.url,
        lastSuccess: page.status_info?.last_success
      })),
      weeklyTrend: weeklyStats.map((day: any) => ({
        date: day.date,
        newPages: day.stats?.new_pages || 0,
        changedPages: day.stats?.changed_pages || 0,
        failedPages: day.stats?.failed_pages || 0
      })),
      healthScore: Math.round(healthScore * 100) / 100,
      summary: {
        totalPages,
        healthyPages,
        deletedCount: deletedPages.length,
        failedCount: failedPages.length,
        recentlyChanged: changedPages.length
      }
    };
    
    return NextResponse.json({ health: healthData });
  } catch (error) {
    console.error('Error fetching page health data:', error);
    return NextResponse.json(
      { error: 'Failed to fetch page health data' },
      { status: 500 }
    );
  }
} 