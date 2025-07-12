'use client';

import { useState, useEffect } from 'react';
import { Plus, RefreshCw, AlertTriangle, Trash2, TrendingUp, TrendingDown } from 'lucide-react';

interface PageHealthData {
  todayStats: {
    pages_crawled: number;
    new_pages: number;
    changed_pages: number;
    failed_pages: number;
  };
  weeklyTrend: Array<{
    date: string;
    newPages: number;
    changedPages: number;
    failedPages: number;
  }>;
  deletedPagesCount: number;
  healthScore: number;
}

interface PageHealthOverviewProps {
  siteId: string;
}

export default function PageHealthOverview({ siteId }: PageHealthOverviewProps) {
  const [healthData, setHealthData] = useState<PageHealthData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (siteId) {
      fetchHealthData();
    }
  }, [siteId]);

  const fetchHealthData = async () => {
    try {
      setLoading(true);
      const [statusResponse, metricsResponse] = await Promise.all([
        fetch(`/api/sites/${siteId}/status`),
        fetch(`/api/sites/${siteId}/metrics`)
      ]);
      
      if (!statusResponse.ok || !metricsResponse.ok) {
        throw new Error('API request failed');
      }
      
      const statusData = await statusResponse.json();
      const metricsData = await metricsResponse.json();
      
      // NO FALLBACKS - Use exact data only
      if (!statusData.status?.todayStats) {
        throw new Error('No today stats available');
      }
      
      const todayStats = statusData.status.todayStats;
      const deletedPagesCount = statusData.status.deletedCount;  // Use actual deleted count
      const healthScore = Math.max(0, 100 - (metricsData.metrics?.errorRate || 0));
      
      setHealthData({
        todayStats,
        weeklyTrend: metricsData.metrics?.dailyStats || [],
        deletedPagesCount,
        healthScore
      });
    } catch (error) {
      console.error('Error fetching health data:', error);
      setHealthData(null);
    } finally {
      setLoading(false);
    }
  };

  const getHealthColor = (score: number) => {
    if (score >= 95) return 'text-green-400';
    if (score >= 85) return 'text-yellow-400';
    return 'text-red-400';
  };

  const getHealthIcon = (score: number) => {
    if (score >= 95) return '🟢';
    if (score >= 85) return '🟡';
    return '🔴';
  };

  const getTrendIcon = (current: number, previous: number) => {
    if (current > previous) return <TrendingUp className="w-3 h-3 text-red-400" />;
    if (current < previous) return <TrendingDown className="w-3 h-3 text-green-400" />;
    return <span className="w-3 h-3 text-gray-400">→</span>;
  };

  if (loading) {
    return (
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <div className="animate-pulse">
          <div className="h-6 bg-gray-700 rounded mb-4 w-1/3"></div>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="h-20 bg-gray-700 rounded"></div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  if (!healthData) {
    return (
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700 text-center">
        <div className="text-gray-400">Failed to load page health data</div>
      </div>
    );
  }

  const { todayStats, weeklyTrend, deletedPagesCount, healthScore } = healthData;
  
  // NO FALLBACKS - Use exact data sources only
  const lastWeekData = weeklyTrend[weeklyTrend.length - 2] || { newPages: 0, changedPages: 0, failedPages: 0 };
  const todayData = weeklyTrend[weeklyTrend.length - 1] || { newPages: 0, changedPages: 0, failedPages: 0 };

  return (
    <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <h3 className="text-lg font-semibold text-white">Page Health Overview</h3>
        <div className="flex items-center gap-2">
          <span className={`text-lg ${getHealthColor(healthScore)}`}>
            {getHealthIcon(healthScore)}
          </span>
          <span className={`font-mono ${getHealthColor(healthScore)}`}>
            {healthScore.toFixed(1)}%
          </span>
        </div>
      </div>

      {/* Today's Stats Grid */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {/* New Pages */}
        <div className="bg-gray-900 rounded-lg p-4">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <Plus className="w-4 h-4 text-green-400" />
              <span className="text-gray-400 text-sm">New Pages</span>
            </div>
            {getTrendIcon(todayData.newPages, lastWeekData.newPages)}
          </div>
          <div className="text-2xl font-mono text-white">{todayStats.new_pages}</div>
          <div className="text-xs text-gray-400">today</div>
        </div>

        {/* Changed Pages */}
        <div className="bg-gray-900 rounded-lg p-4">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <RefreshCw className="w-4 h-4 text-blue-400" />
              <span className="text-gray-400 text-sm">Changed</span>
            </div>
            {getTrendIcon(todayData.changedPages, lastWeekData.changedPages)}
          </div>
          <div className="text-2xl font-mono text-white">{todayStats.changed_pages}</div>
          <div className="text-xs text-gray-400">updated</div>
        </div>

        {/* Failed Pages */}
        <div className="bg-gray-900 rounded-lg p-4">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <AlertTriangle className="w-4 h-4 text-yellow-400" />
              <span className="text-gray-400 text-sm">Failed</span>
            </div>
            {getTrendIcon(todayData.failedPages, lastWeekData.failedPages)}
          </div>
          <div className="text-2xl font-mono text-white">{todayStats.failed_pages}</div>
          <div className="text-xs text-gray-400">errors</div>
        </div>

        {/* Deleted Pages */}
        <div className="bg-gray-900 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Trash2 className="w-4 h-4 text-red-400" />
            <span className="text-gray-400 text-sm">Deleted</span>
          </div>
          <div className="text-2xl font-mono text-white">{deletedPagesCount}</div>
          <div className="text-xs text-gray-400">detected</div>
        </div>
      </div>

      {/* Weekly Summary */}
      {weeklyTrend.length > 0 && (
        <div className="border-t border-gray-700 pt-4">
          <h4 className="text-sm font-medium text-gray-300 mb-3">7-Day Summary</h4>
          <div className="grid grid-cols-3 gap-4 text-center">
            <div>
              <div className="text-lg font-mono text-green-400">
                {weeklyTrend.reduce((sum, day) => sum + day.newPages, 0)}
              </div>
              <div className="text-xs text-gray-400">New Pages</div>
            </div>
            <div>
              <div className="text-lg font-mono text-blue-400">
                {weeklyTrend.reduce((sum, day) => sum + day.changedPages, 0)}
              </div>
              <div className="text-xs text-gray-400">Changed</div>
            </div>
            <div>
              <div className="text-lg font-mono text-yellow-400">
                {weeklyTrend.reduce((sum, day) => sum + (day.failedPages || 0), 0)}
              </div>
              <div className="text-xs text-gray-400">Issues</div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
} 