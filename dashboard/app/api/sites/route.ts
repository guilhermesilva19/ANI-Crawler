import { NextResponse } from 'next/server';
import { getAllSites } from '@/lib/mongodb';

export async function GET() {
  try {
    const sites = await getAllSites();
    return NextResponse.json({ sites });
  } catch (error) {
    console.error('Error fetching sites:', error);
    return NextResponse.json(
      { error: 'Failed to fetch sites' },
      { status: 500 }
    );
  }
} 