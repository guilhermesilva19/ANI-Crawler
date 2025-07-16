'use client';

import { useState, useEffect } from 'react';
import { Globe, Clock, Zap, Activity } from 'lucide-react';

interface SiteStatusData {
  siteId: string;
  isActive: boolean;
  totalPages: number;
  completedPages: number;
  remainingPages: number;
  progressPercent: number;
  currentSpeed: number;
  etaHours: number | null;
  statusCounts: {
    visited: number;
    remaining: number;
    failed: number;
    deleted: number;
    changed: number;
  };
  todayProcessed: number;
}

interface SiteStatusProps {
  siteId: string;
}

export default function SiteStatus({ siteId }: SiteStatusProps) {
  const [data, setData] = useState<SiteStatusData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (siteId) {
      fetchData();
      
      // Auto-refresh every 30 seconds
      const interval = setInterval(fetchData, 30000);
      return () => clearInterval(interval);
    }
  }, [siteId]);

  const fetchData = async () => {
    try {
      setLoading(true);
      
      // Get real status data and URL counts
      const [statusResponse, urlResponse] = await Promise.all([
        fetch(`/api/sites/${siteId}/status`),
        fetch(`/api/sites/${siteId}/urls?limit=1`) // Just get counts, not actual URLs
      ]);
      
      if (!statusResponse.ok || !urlResponse.ok) {
        throw new Error('Failed to fetch data');
      }
      
      const statusData = await statusResponse.json();
      const urlData = await urlResponse.json();
      
      setData({
        siteId,
        isActive: statusData.status.isActive,
        totalPages: statusData.status.totalPages,
        completedPages: statusData.status.completedPages,
        remainingPages: statusData.status.remainingPages,
        progressPercent: statusData.status.progressPercent,
        currentSpeed: statusData.status.currentSpeed,
        etaHours: statusData.status.etaHours,
        statusCounts: {
          visited: urlData.statusCounts.visited,
          remaining: urlData.statusCounts.remaining,
          failed: urlData.statusCounts.failed,
          deleted: urlData.statusCounts.deleted,
          changed: urlData.statusCounts.changed
        },
        todayProcessed: statusData.status.todayStats.pages_crawled || 0
      });
    } catch (error) {
      console.error('Error fetching site status:', error);
    } finally {
      setLoading(false);
    }
  };

  const formatETA = (hours: number | null) => {
    if (!hours) return 'Unknown';
    if (hours < 1) return `${Math.round(hours * 60)} minutes`;
    if (hours < 24) return `${Math.round(hours)} hours`;
    const days = Math.round(hours / 24);
    return `${days} ${days === 1 ? 'day' : 'days'}`;
  };

  const formatDomain = (siteId: string) => {
    return siteId.replace(/_/g, '.').replace('-gov-', '.gov.');
  };

  if (loading) {
    return (
      <div className="bg-gray-800 rounded-xl p-8 border border-gray-700">
        <div className="animate-pulse">
          <div className="h-6 bg-gray-700 rounded w-48 mb-6"></div>
          <div className="h-4 bg-gray-700 rounded w-full mb-8"></div>
          <div className="space-y-4">
            <div className="h-4 bg-gray-700 rounded w-32"></div>
            <div className="h-4 bg-gray-700 rounded w-28"></div>
            <div className="h-4 bg-gray-700 rounded w-36"></div>
          </div>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="bg-gray-800 rounded-xl p-8 border border-gray-700">
        <div className="text-center text-gray-400">
          Failed to load site status
        </div>
      </div>
    );
  }

  return (
    <div className="bg-gray-800 rounded-xl p-8 border border-gray-700">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <Globe className="w-6 h-6 text-blue-400" />
        <h2 className="text-xl font-semibold text-white">
          {formatDomain(data.siteId)}
        </h2>
        <div className={`ml-auto flex items-center gap-2 px-3 py-1 rounded-full text-sm ${
          data.isActive 
            ? 'bg-green-900 text-green-300' 
            : 'bg-gray-700 text-gray-400'
        }`}>
          <Activity className="w-3 h-3" />
          {data.isActive ? 'Active' : 'Idle'}
        </div>
      </div>

      {/* Progress Bar */}
      <div className="mb-8">
        <div className="flex items-center justify-between mb-2">
          <span className="text-gray-300 font-medium">Crawl Progress</span>
          <span className="text-white font-mono text-lg">{data.progressPercent}%</span>
        </div>
        <div className="w-full bg-gray-700 rounded-full h-3">
          <div 
            className="bg-gradient-to-r from-blue-500 to-blue-400 h-3 rounded-full transition-all duration-300"
            style={{ width: `${data.progressPercent}%` }}
          ></div>
        </div>
        <div className="text-sm text-gray-400 mt-2">
          {data.completedPages.toLocaleString()} / {data.totalPages.toLocaleString()} pages crawled
        </div>
      </div>

      {/* Status Counts */}
      <div className="mb-8">
        <h3 className="text-lg font-medium text-white mb-4">📊 Status Counts</h3>
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-3">
            <div className="flex justify-between">
              <span className="text-gray-400">• Visited (crawled):</span>
              <span className="text-white font-mono">{data.statusCounts.visited.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-blue-400">• Discovered (new):</span>
              <span className="text-blue-300 font-mono">{data.statusCounts.remaining.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-green-400">• Changed:</span>
              <span className="text-green-300 font-mono">{data.statusCounts.changed.toLocaleString()}</span>
            </div>
          </div>
          <div className="space-y-3">
            <div className="flex justify-between">
              <span className="text-orange-400">• Failed:</span>
              <span className="text-orange-300 font-mono">{data.statusCounts.failed.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-red-400">• Deleted:</span>
              <span className="text-red-300 font-mono">{data.statusCounts.deleted.toLocaleString()}</span>
            </div>
          </div>
        </div>
      </div>

      {/* Performance Info */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-6">
        <div className="text-center">
          <div className="flex items-center justify-center gap-2 mb-2">
            <Zap className="w-4 h-4 text-yellow-400" />
            <span className="text-gray-400 text-sm">Current Speed</span>
          </div>
          <div className="text-xl font-mono text-white">
            {data.currentSpeed} <span className="text-sm text-gray-400">pages/hour</span>
          </div>
        </div>

        <div className="text-center">
          <div className="flex items-center justify-center gap-2 mb-2">
            <Clock className="w-4 h-4 text-blue-400" />
            <span className="text-gray-400 text-sm">ETA</span>
          </div>
          <div className="text-xl font-mono text-white">
            {formatETA(data.etaHours)}
          </div>
        </div>

        <div className="text-center">
          <div className="flex items-center justify-center gap-2 mb-2">
            <Activity className="w-4 h-4 text-green-400" />
            <span className="text-gray-400 text-sm">Today</span>
          </div>
          <div className="text-xl font-mono text-white">
            {data.todayProcessed} <span className="text-sm text-gray-400">processed</span>
          </div>
        </div>
      </div>
    </div>
  );
} 