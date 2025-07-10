'use client';

import { useState, useEffect } from 'react';
import { Clock, CheckCircle, AlertCircle, Plus, RefreshCw, XCircle, Trash2 } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';

interface Activity {
  id: string;
  timestamp: string;
  type: 'audit' | 'crawl';
  action: string;
  url: string;
  details: string;
  status: 'success' | 'warning' | 'error';
}

interface ActivityTimelineProps {
  siteId: string;
}

export default function ActivityTimeline({ siteId }: ActivityTimelineProps) {
  const [activities, setActivities] = useState<Activity[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (siteId) {
      fetchActivities();
      
      // Auto-refresh every 10 seconds when crawler is active
      const interval = setInterval(fetchActivities, 10000);
      return () => clearInterval(interval);
    }
  }, [siteId]);

  const fetchActivities = async () => {
    try {
      // Only show loading for initial fetch, not auto-refreshes
      if (activities.length === 0) {
        setLoading(true);
      }
      
      const response = await fetch(`/api/sites/${siteId}/activity`);
      if (!response.ok) {
        throw new Error(`Failed to fetch activities: ${response.status}`);
      }
      
      const data = await response.json();
      const newActivities = data.activities || [];
      
      if (newActivities.length === 0) {
        throw new Error('No activity data received');
      }
      
      setActivities(newActivities);
    } catch (error) {
      console.error('Error fetching activities:', error);
      // Don't clear existing activities on error during auto-refresh
      if (activities.length === 0) {
        setActivities([]);
      }
    } finally {
      setLoading(false);
    }
  };

  const getActivityIcon = (activity: Activity) => {
    switch (activity.action) {
      case 'new_page':
        return <Plus className="w-4 h-4 text-green-400" />;
      case 'page_changed':
        return <RefreshCw className="w-4 h-4 text-blue-400" />;
      case 'page_deleted':
        return <Trash2 className="w-4 h-4 text-red-400" />;
      case 'page_failed':
        return <XCircle className="w-4 h-4 text-red-400" />;
      default:
        if (activity.status === 'warning') {
          return <AlertCircle className="w-4 h-4 text-yellow-400" />;
        }
        if (activity.status === 'error') {
          return <XCircle className="w-4 h-4 text-red-400" />;
        }
        return <CheckCircle className="w-4 h-4 text-blue-400" />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'success': return 'border-blue-400';
      case 'warning': return 'border-yellow-400';
      case 'error': return 'border-red-400';
      default: return 'border-gray-400';
    }
  };

  const formatUrl = (url: string) => {
    const maxLength = 50;
    if (url.length <= maxLength) return url;
    return url.slice(0, maxLength) + '...';
  };

  if (loading) {
    return (
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <h3 className="text-lg font-semibold text-white mb-4">Recent Activity</h3>
        <div className="space-y-4">
          {[1, 2, 3, 4, 5].map((i) => (
            <div key={i} className="animate-pulse flex gap-3">
              <div className="w-8 h-8 bg-gray-700 rounded-full"></div>
              <div className="flex-1">
                <div className="h-4 bg-gray-700 rounded w-3/4 mb-2"></div>
                <div className="h-3 bg-gray-700 rounded w-1/2"></div>
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold text-white">Recent Activity</h3>
        <button
          onClick={fetchActivities}
          className="text-sm text-gray-400 hover:text-white transition-colors"
        >
          Refresh
        </button>
      </div>
      
      {activities.length === 0 ? (
        <div className="text-center py-8">
          <Clock className="w-8 h-8 text-gray-400 mx-auto mb-2" />
          <p className="text-gray-400">No recent activity</p>
        </div>
      ) : (
        <div className="space-y-4 max-h-96 overflow-y-auto">
          {activities.map((activity, index) => (
            <div key={activity.id} className="flex gap-3">
              {/* Timeline indicator */}
              <div className="flex flex-col items-center">
                <div className={`w-8 h-8 rounded-full border-2 ${getStatusColor(activity.status)} bg-gray-900 flex items-center justify-center`}>
                  {getActivityIcon(activity)}
                </div>
                {index < activities.length - 1 && (
                  <div className="w-px h-6 bg-gray-600 mt-2"></div>
                )}
              </div>
              
              {/* Activity content */}
              <div className="flex-1 min-w-0 pb-4">
                <div className="flex items-start justify-between">
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-white">
                      <span className="font-medium">
                        {activity.action === 'new_page' ? 'New page discovered' : 
                         activity.action === 'page_changed' ? 'Page content changed' :
                         activity.action === 'page_deleted' ? 'Page deleted/removed' :
                         activity.action === 'page_failed' ? 'Page failed to load' :
                         activity.action === 'page_scan' ? 'Page scanned' : 'Page crawled'}
                      </span>
                    </p>
                    <p className="text-sm text-gray-400 truncate font-mono">
                      {formatUrl(activity.url)}
                    </p>
                    {activity.details && (
                      <p className="text-xs text-gray-500 mt-1">{activity.details}</p>
                    )}
                  </div>
                  <time className="text-xs text-gray-500 ml-2 whitespace-nowrap">
                    {formatDistanceToNow(new Date(activity.timestamp), { addSuffix: true })}
                  </time>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
} 