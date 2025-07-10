'use client';

import { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar } from 'recharts';

interface MetricsData {
  speedTrend: Array<{
    time: number;
    speed: number;
    count: number;
  }>;
  dailyStats: Array<{
    date: string;
    newPages: number;
    totalPages: number;
    changedPages: number;
  }>;
  errorRate: number;
  performance: {
    avgSpeed: number;
    peakSpeed: number;
    minSpeed: number;
  };
}

interface PerformanceChartsProps {
  siteId: string;
}

export default function PerformanceCharts({ siteId }: PerformanceChartsProps) {
  const [metrics, setMetrics] = useState<MetricsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeChart, setActiveChart] = useState<'speed' | 'discovery'>('speed');

  useEffect(() => {
    if (siteId) {
      fetchMetrics();
    }
  }, [siteId]);

  const fetchMetrics = async () => {
    try {
      setLoading(true);
      const response = await fetch(`/api/sites/${siteId}/metrics`);
      const data = await response.json();
      setMetrics(data.metrics);
    } catch (error) {
      console.error('Error fetching metrics:', error);
    } finally {
      setLoading(false);
    }
  };

  const formatTime = (timestamp: number) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
  };

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  if (loading) {
    return (
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <div className="animate-pulse">
          <div className="h-6 bg-gray-700 rounded mb-4 w-1/3"></div>
          <div className="h-64 bg-gray-700 rounded"></div>
        </div>
      </div>
    );
  }

  if (!metrics) {
    return (
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700 text-center">
        <div className="text-gray-400">Failed to load metrics</div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Performance Overview Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <div className="text-sm text-gray-400 mb-1">Average Speed</div>
          <div className="text-2xl font-mono text-white">{metrics.performance.avgSpeed}</div>
          <div className="text-sm text-gray-400">pages/hour</div>
        </div>
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <div className="text-sm text-gray-400 mb-1">Peak Speed</div>
          <div className="text-2xl font-mono text-green-400">{metrics.performance.peakSpeed}</div>
          <div className="text-sm text-gray-400">pages/hour</div>
        </div>
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <div className="text-sm text-gray-400 mb-1">Error Rate</div>
          <div className={`text-2xl font-mono ${metrics.errorRate > 5 ? 'text-red-400' : 'text-green-400'}`}>
            {metrics.errorRate}%
          </div>
          <div className="text-sm text-gray-400">of requests</div>
        </div>
      </div>

      {/* Chart Section */}
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-lg font-semibold text-white">Performance Analytics</h3>
          <div className="flex gap-2">
            <button
              onClick={() => setActiveChart('speed')}
              className={`px-3 py-1 rounded text-sm transition-colors ${
                activeChart === 'speed' 
                  ? 'bg-blue-600 text-white' 
                  : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
              }`}
            >
              Speed Trend
            </button>
            <button
              onClick={() => setActiveChart('discovery')}
              className={`px-3 py-1 rounded text-sm transition-colors ${
                activeChart === 'discovery' 
                  ? 'bg-blue-600 text-white' 
                  : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
              }`}
            >
              Discovery Trend
            </button>
          </div>
        </div>

        <div className="h-64">
          {activeChart === 'speed' ? (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={metrics.speedTrend}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis 
                  dataKey="time" 
                  tickFormatter={formatTime}
                  stroke="#9CA3AF"
                  fontSize={12}
                />
                <YAxis 
                  stroke="#9CA3AF"
                  fontSize={12}
                  label={{ value: 'Pages/Hour', angle: -90, position: 'insideLeft', style: { textAnchor: 'middle', fill: '#9CA3AF' } }}
                />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: '#1F2937', 
                    border: '1px solid #374151',
                    borderRadius: '6px',
                    color: '#fff'
                  }}
                  labelFormatter={(value) => `Time: ${formatTime(value as number)}`}
                  formatter={(value, name) => [
                    `${value} pages/hour`,
                    'Speed'
                  ]}
                />
                <Line 
                  type="monotone" 
                  dataKey="speed" 
                  stroke="#3B82F6" 
                  strokeWidth={2}
                  dot={{ fill: '#3B82F6', strokeWidth: 0, r: 3 }}
                  activeDot={{ r: 5, stroke: '#3B82F6', strokeWidth: 2, fill: '#1F2937' }}
                />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={metrics.dailyStats}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis 
                  dataKey="date" 
                  tickFormatter={formatDate}
                  stroke="#9CA3AF"
                  fontSize={12}
                />
                <YAxis 
                  stroke="#9CA3AF"
                  fontSize={12}
                  label={{ value: 'Pages', angle: -90, position: 'insideLeft', style: { textAnchor: 'middle', fill: '#9CA3AF' } }}
                />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: '#1F2937', 
                    border: '1px solid #374151',
                    borderRadius: '6px',
                    color: '#fff'
                  }}
                  labelFormatter={(value) => `Date: ${formatDate(value as string)}`}
                />
                <Bar dataKey="newPages" fill="#10B981" name="New Pages" />
                <Bar dataKey="changedPages" fill="#F59E0B" name="Changed Pages" />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Chart Legend */}
        <div className="mt-4 flex justify-center gap-6 text-sm">
          {activeChart === 'speed' ? (
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 bg-blue-500 rounded"></div>
              <span className="text-gray-300">Crawl Speed (pages/hour)</span>
            </div>
          ) : (
            <>
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 bg-green-500 rounded"></div>
                <span className="text-gray-300">New Pages</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 bg-yellow-500 rounded"></div>
                <span className="text-gray-300">Changed Pages</span>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
} 