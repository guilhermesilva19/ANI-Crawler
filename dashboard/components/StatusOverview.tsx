'use client';

import { useState, useEffect } from 'react';
import { Activity, Clock, Target, Zap } from 'lucide-react';

interface StatusData {
  siteId: string;
  isActive: boolean;
  totalPages: number;
  completedPages: number;
  remainingPages: number;
  progressPercent: number;
  currentSpeed: number;
  avgCrawlTime: string;
  etaHours: number | null;
  cycleInfo: {
    number: number;
    type: string;
    durationDays: number;
  };
  todayStats: {
    pages_crawled: number;
    new_pages: number;
    changed_pages: number;
    errors: number;
  };
}

interface StatusOverviewProps {
  siteId: string;
}

export default function StatusOverview({ siteId }: StatusOverviewProps) {
  const [status, setStatus] = useState<StatusData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (siteId) {
      fetchStatus();
      
      // Auto-refresh every 30 seconds to update active status
      const interval = setInterval(fetchStatus, 30000);
      return () => clearInterval(interval);
    }
  }, [siteId]);

  const fetchStatus = async () => {
    try {
      setLoading(true);
      const response = await fetch(`/api/sites/${siteId}/status`);
      const data = await response.json();
      setStatus(data.status);
    } catch (error) {
      console.error('Error fetching status:', error);
    } finally {
      setLoading(false);
    }
  };

  const getPerformanceGrade = (speed: number) => {
    if (speed >= 300) return { grade: 'Excellent', color: 'text-green-400', icon: '🚀' };
    if (speed >= 200) return { grade: 'Good', color: 'text-blue-400', icon: '⚡' };
    if (speed >= 120) return { grade: 'Normal', color: 'text-yellow-400', icon: '✅' };
    if (speed >= 60) return { grade: 'Slow', color: 'text-orange-400', icon: '🐌' };
    return { grade: 'Very Slow', color: 'text-red-400', icon: '⚠️' };
  };

  const formatETA = (hours: number | null) => {
    if (!hours) return 'Calculating...';
    if (hours < 1) return `${Math.round(hours * 60)} minutes`;
    if (hours < 24) return `${hours.toFixed(1)} hours`;
    const days = Math.floor(hours / 24);
    const remainingHours = hours % 24;
    return `${days}d ${remainingHours.toFixed(0)}h`;
  };

  const getProgressBar = (percent: number) => {
    const filled = Math.round((percent / 100) * 20);
    const empty = 20 - filled;
    return '█'.repeat(filled) + '░'.repeat(empty);
  };

  if (loading) {
    return (
      <div className="bg-gray-800 rounded-xl p-6">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-700 rounded mb-4"></div>
          <div className="space-y-3">
            <div className="h-4 bg-gray-700 rounded w-3/4"></div>
            <div className="h-4 bg-gray-700 rounded w-1/2"></div>
          </div>
        </div>
      </div>
    );
  }

  if (!status) {
    return (
      <div className="bg-gray-800 rounded-xl p-6 text-center">
        <div className="text-gray-400">Failed to load status</div>
      </div>
    );
  }

  const perfGrade = getPerformanceGrade(status.currentSpeed);

  return (
    <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div className={`w-3 h-3 rounded-full ${status.isActive ? 'bg-green-400 animate-pulse' : 'bg-gray-400'}`}></div>
          <h2 className="text-xl font-bold text-white">
            {status.isActive ? 'ACTIVE' : 'IDLE'}: 
            {status.isActive ? ' Crawling' : ' Waiting'}
          </h2>
        </div>
        <div className="text-sm text-gray-400">
          {status.cycleInfo.type} Cycle {status.cycleInfo.number} • Day {status.cycleInfo.durationDays + 1}
          {!status.isActive && (
            <div className="text-xs text-gray-500 mt-1">
              No activity in last 5 minutes
            </div>
          )}
        </div>
      </div>

      {/* Progress Bar */}
      <div className="mb-6">
        <div className="flex items-center justify-between mb-2">
          <span className="text-gray-400">Progress</span>
          <span className="text-white font-mono">
            {status.completedPages.toLocaleString()}/{status.totalPages.toLocaleString()} pages
          </span>
        </div>
        <div className="bg-gray-700 rounded-full h-2 mb-2">
          <div 
            className="bg-blue-500 h-2 rounded-full transition-all duration-300"
            style={{ width: `${Math.min(status.progressPercent, 100)}%` }}
          ></div>
        </div>
        <div className="flex justify-between text-sm">
          <span className="text-blue-400 font-mono">{status.progressPercent}%</span>
          <span className="text-gray-400">{status.remainingPages.toLocaleString()} remaining</span>
        </div>
      </div>

      {/* Key Metrics Grid */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Current Speed */}
        <div className="bg-gray-900 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Zap className="w-4 h-4 text-yellow-400" />
            <span className="text-gray-400 text-sm">Speed</span>
          </div>
          <div className="text-2xl font-mono text-white">{status.currentSpeed}</div>
          <div className={`text-sm ${perfGrade.color} flex items-center gap-1`}>
            <span>{perfGrade.icon}</span>
            <span>{perfGrade.grade}</span>
          </div>
        </div>

        {/* Average Time */}
        <div className="bg-gray-900 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Clock className="w-4 h-4 text-blue-400" />
            <span className="text-gray-400 text-sm">Avg Time</span>
          </div>
          <div className="text-2xl font-mono text-white">{status.avgCrawlTime}s</div>
          <div className="text-sm text-gray-400">per page</div>
        </div>

        {/* ETA */}
        <div className="bg-gray-900 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Target className="w-4 h-4 text-green-400" />
            <span className="text-gray-400 text-sm">ETA</span>
          </div>
          <div className="text-lg font-mono text-white">{formatETA(status.etaHours)}</div>
          <div className="text-sm text-gray-400">completion</div>
        </div>

        {/* Today's New Pages */}
        <div className="bg-gray-900 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Activity className="w-4 h-4 text-green-400" />
            <span className="text-gray-400 text-sm">New Pages</span>
          </div>
          <div className="text-2xl font-mono text-white">{status.todayStats.new_pages}</div>
          <div className="text-sm text-gray-400">discovered today</div>
        </div>
      </div>
    </div>
  );
} 