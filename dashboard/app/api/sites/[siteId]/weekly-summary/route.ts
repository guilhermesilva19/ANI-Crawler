import { NextResponse } from 'next/server';
import { getDailyStats, getUrlStates } from '@/lib/mongodb';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ siteId: string }> }
) {
  try {
    const { siteId } = await params;
    
    // Convert site ID format (dashboard uses hyphens, database uses underscores)
    const dbSiteId = siteId.replace(/-/g, '_');
    
    // Current AEST date for proper timezone handling
    const now = new Date();
    const aestNow = new Date(now.toLocaleString('en-US', { timeZone: 'Australia/Sydney' }));
    
    // Get current totals from URL states (CONSISTENT with status route)
    const urlStates = await getUrlStates();
    const [visitedCount, remainingCount, inProgressCount, totalDiscoveredPages, deletedCount, failedCount, changedCount] = await Promise.all([
      urlStates.countDocuments({ site_id: dbSiteId, status: 'visited' }),
      urlStates.countDocuments({ site_id: dbSiteId, status: 'remaining' }),
      urlStates.countDocuments({ site_id: dbSiteId, status: 'in_progress' }),
      urlStates.countDocuments({ site_id: dbSiteId }), // TOTAL discovered pages (all statuses)
      urlStates.countDocuments({ 
        site_id: dbSiteId, 
        'status_info.status': { $in: [404, 410] },
        'status_info.error_count': { $gte: 2 }
      }),
      urlStates.countDocuments({ 
        site_id: dbSiteId, 
        'status_info.status': { $gte: 400 }
      }),
      urlStates.countDocuments({ 
        site_id: dbSiteId, 
        last_change: { $exists: true }
      })
    ]);
    
        // Get last 7 days of activity data (AEST timezone)
    const sevenDaysAgo = new Date(aestNow);
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    const startDateStr = sevenDaysAgo.toISOString().split('T')[0];

    const dailyStats = await getDailyStats();
    
    // Get daily stats for ONLY the selected site
    const last7DaysData = await dailyStats.find({ 
      site_id: dbSiteId,
      date: { $gte: startDateStr }
    })
    .sort({ date: 1 })
    .toArray();

    // Get ACTUAL discovery data by first_seen date (last 7 days)
    const discoveryByDate = await urlStates.aggregate([
      {
        $match: {
          site_id: dbSiteId,
          first_seen: { 
            $gte: sevenDaysAgo,
            $exists: true 
          }
        }
      },
      {
        $addFields: {
          discovery_date: {
            $dateToString: {
              format: "%Y-%m-%d",
              date: "$first_seen",
              timezone: "Australia/Sydney"
            }
          }
        }
      },
      {
        $group: {
          _id: "$discovery_date",
          new_pages_discovered: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();

    // Sum up last 7 days activity (REAL discovery + processing activity)
    let last7DaysActivity = {
      newPages: 0,
      changedPages: 0, 
      failedPages: 0,
      deletedPages: 0,
      totalActivity: 0
    };

    // Add processing activity from daily stats
    last7DaysData.forEach((day: any) => {
      const stats = day.stats || {};
      last7DaysActivity.changedPages += stats.changed_pages || 0;
      last7DaysActivity.failedPages += stats.failed_pages || 0;
      last7DaysActivity.deletedPages += stats.deleted_pages || 0;
    });

    // Add REAL page discovery data
    discoveryByDate.forEach((discovery: any) => {
      last7DaysActivity.newPages += discovery.new_pages_discovered || 0;
    });

    last7DaysActivity.totalActivity = last7DaysActivity.newPages + last7DaysActivity.changedPages + 
                                      last7DaysActivity.failedPages + last7DaysActivity.deletedPages;
    
    const trends = {
      newPages: {
        current: totalDiscoveredPages,  // Current total discovered
        previous: Math.max(0, totalDiscoveredPages - last7DaysActivity.newPages),
        change: last7DaysActivity.newPages,   // New pages discovered in last 7 days
        percentChange: 0 // Not meaningful for 7-day summary
      },
      changedPages: {
        current: changedCount,  // Total pages with changes ever
        previous: Math.max(0, changedCount - last7DaysActivity.changedPages),
        change: last7DaysActivity.changedPages,   // Pages changed in last 7 days
        percentChange: 0 // Not meaningful for 7-day summary
      },
      failedPages: {
        current: failedCount,  // Total failed pages
        previous: Math.max(0, failedCount - last7DaysActivity.failedPages),
        change: last7DaysActivity.failedPages,   // Pages failed in last 7 days
        percentChange: 0 // Not meaningful for 7-day summary
      },
      deletedPages: {
        current: deletedCount,  // Total deleted pages
        previous: Math.max(0, deletedCount - last7DaysActivity.deletedPages),
        change: last7DaysActivity.deletedPages,   // Pages deleted in last 7 days
        percentChange: 0 // Not meaningful for 7-day summary
      },
      totalActivity: {
        current: last7DaysActivity.totalActivity,  // Total activity in last 7 days
        previous: 0,  // Not meaningful for total activity
        change: last7DaysActivity.totalActivity,  // Total activity in last 7 days
        percentChange: 0 // Not meaningful for 7-day summary
      }
    };
    
    return NextResponse.json({
      last7DaysActivity,
      trends,
      totalDays: last7DaysData.length,
      debug: {
        realTotals: {
          totalDiscovered: totalDiscoveredPages,
          visited: visitedCount,
          remaining: remainingCount,
          deleted: deletedCount,
          failed: failedCount,
          changed: changedCount
        },
        currentAEST: aestNow.toISOString(),
        timezone: "Australia/Sydney",
        explanation: "Shows last 7 days of activity in AEST timezone - simple and clean"
      }
    });
  } catch (error) {
    console.error('Error fetching weekly summary:', error);
    return NextResponse.json(
      { error: 'Failed to fetch weekly summary' },
      { status: 500 }
    );
  }
} 