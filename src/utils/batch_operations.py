"""High-performance batch operations for Drive uploads and database writes."""

import threading
import time
import queue
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import weakref

__all__ = ['BatchUploadManager', 'BatchDatabaseManager', 'BatchOperation']


@dataclass
class BatchOperation:
    """Represents a batch operation to be processed."""
    id: str
    operation_type: str  # 'upload', 'database_write', etc.
    data: Dict[str, Any]
    callback: Optional[Callable] = None
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class BatchUploadManager:
    """High-performance batch manager for Google Drive uploads."""
    
    def __init__(self, 
                 drive_service,
                 batch_size: int = 5,
                 batch_timeout: float = 30.0,
                 max_concurrent_uploads: int = 3):
        """Initialize batch upload manager."""
        self.drive_service = drive_service
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.max_concurrent_uploads = max_concurrent_uploads
        
        # Queue and processing
        self._upload_queue = queue.Queue()
        self._processing = False
        self._lock = threading.RLock()
        self._worker_thread = None
        
        # Performance tracking
        self._stats = {
            'total_queued': 0,
            'total_processed': 0,
            'successful_uploads': 0,
            'failed_uploads': 0,
            'batches_processed': 0,
            'avg_batch_time': 0.0,
            'queue_size': 0
        }
        
        # Start processing
        self.start_processing()
    
    def queue_upload(self, 
                    file_path: str, 
                    folder_id: str, 
                    callback: Optional[Callable] = None,
                    priority: int = 0) -> str:
        """Queue a file upload for batch processing."""
        operation_id = f"upload_{int(time.time() * 1000000)}"
        
        operation = BatchOperation(
            id=operation_id,
            operation_type='upload',
            data={
                'file_path': file_path,
                'folder_id': folder_id,
                'priority': priority
            },
            callback=callback
        )
        
        self._upload_queue.put(operation)
        self._stats['total_queued'] += 1
        self._stats['queue_size'] = self._upload_queue.qsize()
        
        print(f"📤 Queued upload: {file_path} (queue size: {self._stats['queue_size']})")
        return operation_id
    
    def start_processing(self) -> None:
        """Start the batch processing worker."""
        if self._processing:
            return
            
        self._processing = True
        self._worker_thread = threading.Thread(target=self._process_batches, daemon=True)
        self._worker_thread.start()
        print("🚀 Batch upload manager started")
    
    def stop_processing(self) -> None:
        """Stop the batch processing worker."""
        self._processing = False
        if self._worker_thread:
            self._worker_thread.join(timeout=5.0)
        print("🛑 Batch upload manager stopped")
    
    def _process_batches(self) -> None:
        """Main worker loop for processing upload batches."""
        batch = []
        last_batch_time = time.time()
        
        while self._processing:
            try:
                # Try to get an operation from queue
                try:
                    operation = self._upload_queue.get(timeout=1.0)
                    batch.append(operation)
                except queue.Empty:
                    # Check if we should process partial batch due to timeout
                    if batch and (time.time() - last_batch_time) >= self.batch_timeout:
                        self._process_upload_batch(batch)
                        batch = []
                        last_batch_time = time.time()
                    continue
                
                # Process batch if it's full or timeout reached
                if (len(batch) >= self.batch_size or 
                    (batch and (time.time() - last_batch_time) >= self.batch_timeout)):
                    
                    self._process_upload_batch(batch)
                    batch = []
                    last_batch_time = time.time()
                    
            except Exception as e:
                print(f"❌ Batch processing error: {e}")
                #time.sleep(1.0)
        
        # Process remaining batch
        if batch:
            self._process_upload_batch(batch)
    
    def _process_upload_batch(self, batch: List[BatchOperation]) -> None:
        """Process a batch of upload operations concurrently."""
        if not batch:
            return
            
        start_time = time.time()
        print(f"📦 Processing upload batch: {len(batch)} files")
        
        # Sort by priority (higher priority first)
        batch.sort(key=lambda op: op.data.get('priority', 0), reverse=True)
        
        # Process uploads concurrently
        with ThreadPoolExecutor(max_workers=self.max_concurrent_uploads) as executor:
            # Submit all uploads
            future_to_operation = {
                executor.submit(self._upload_single_file, operation): operation
                for operation in batch
            }
            
            # Collect results
            for future in as_completed(future_to_operation):
                operation = future_to_operation[future]
                try:
                    success = future.result()
                    if success:
                        self._stats['successful_uploads'] += 1
                        if operation.callback:
                            operation.callback(True, operation.id)
                    else:
                        self._handle_upload_failure(operation)
                        
                except Exception as e:
                    print(f"❌ Upload exception for {operation.data['file_path']}: {e}")
                    self._handle_upload_failure(operation)
                finally:
                    self._stats['total_processed'] += 1
        
        # Update statistics
        batch_time = time.time() - start_time
        self._stats['batches_processed'] += 1
        self._stats['avg_batch_time'] = (
            (self._stats['avg_batch_time'] * (self._stats['batches_processed'] - 1) + batch_time) 
            / self._stats['batches_processed']
        )
        self._stats['queue_size'] = self._upload_queue.qsize()
        
        print(f"✅ Batch completed in {batch_time:.2f}s: {len(batch)} files")
    
    def _upload_single_file(self, operation: BatchOperation) -> bool:
        """Upload a single file with error handling."""
        try:
            file_path = operation.data['file_path']
            folder_id = operation.data['folder_id']
            
            if not self.drive_service:
                return False
            
            # Add delay to prevent API quota issues
            #time.sleep(1.0)
            
            result = self.drive_service.upload_file(file_path, folder_id)
            return result is not None
            
        except Exception as e:
            print(f"❌ Upload error for {operation.data['file_path']}: {e}")
            return False
    
    def _handle_upload_failure(self, operation: BatchOperation) -> None:
        """Handle upload failure with retry logic."""
        operation.retry_count += 1
        
        if operation.retry_count <= operation.max_retries:
            # Requeue for retry
            print(f"🔄 Retrying upload {operation.retry_count}/{operation.max_retries}: {operation.data['file_path']}")
            self._upload_queue.put(operation)
        else:
            # Max retries reached
            print(f"❌ Upload failed permanently: {operation.data['file_path']}")
            self._stats['failed_uploads'] += 1
            if operation.callback:
                operation.callback(False, operation.id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get batch upload statistics."""
        return {
            **self._stats,
            'queue_size': self._upload_queue.qsize(),
            'success_rate': (
                self._stats['successful_uploads'] / max(1, self._stats['total_processed'])
            ) if self._stats['total_processed'] > 0 else 0.0
        }


class BatchDatabaseManager:
    """High-performance batch manager for database operations."""
    
    def __init__(self, 
                 db_connection,
                 batch_size: int = 50,
                 batch_timeout: float = 10.0):
        """Initialize batch database manager."""
        self.db = db_connection
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        # Queue and processing
        self._db_queue = queue.Queue()
        self._processing = False
        self._lock = threading.RLock()
        self._worker_thread = None
        
        # Performance tracking
        self._stats = {
            'total_queued': 0,
            'total_processed': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'batches_processed': 0,
            'avg_batch_time': 0.0
        }
        
        # Start processing
        self.start_processing()
    
    def queue_database_operation(self, 
                                collection: str,
                                operation_type: str,  # 'insert', 'update', 'delete'
                                data: Dict[str, Any],
                                callback: Optional[Callable] = None) -> str:
        """Queue a database operation for batch processing."""
        operation_id = f"db_{int(time.time() * 1000000)}"
        
        operation = BatchOperation(
            id=operation_id,
            operation_type='database',
            data={
                'collection': collection,
                'operation_type': operation_type,
                'data': data
            },
            callback=callback
        )
        
        self._db_queue.put(operation)
        self._stats['total_queued'] += 1
        
        return operation_id
    
    def start_processing(self) -> None:
        """Start the batch processing worker."""
        if self._processing:
            return
            
        self._processing = True
        self._worker_thread = threading.Thread(target=self._process_db_batches, daemon=True)
        self._worker_thread.start()
        print("🗄️  Batch database manager started")
    
    def stop_processing(self) -> None:
        """Stop the batch processing worker."""
        self._processing = False
        if self._worker_thread:
            self._worker_thread.join(timeout=5.0)
        print("🛑 Batch database manager stopped")
    
    def _process_db_batches(self) -> None:
        """Main worker loop for processing database batches."""
        batch = []
        last_batch_time = time.time()
        
        while self._processing:
            try:
                # Try to get an operation from queue
                try:
                    operation = self._db_queue.get(timeout=1.0)
                    batch.append(operation)
                except queue.Empty:
                    # Check if we should process partial batch due to timeout
                    if batch and (time.time() - last_batch_time) >= self.batch_timeout:
                        self._process_database_batch(batch)
                        batch = []
                        last_batch_time = time.time()
                    continue
                
                # Process batch if it's full or timeout reached
                if (len(batch) >= self.batch_size or 
                    (batch and (time.time() - last_batch_time) >= self.batch_timeout)):
                    
                    self._process_database_batch(batch)
                    batch = []
                    last_batch_time = time.time()
                    
            except Exception as e:
                print(f"❌ Database batch processing error: {e}")
                #time.sleep(1.0)
        
        # Process remaining batch
        if batch:
            self._process_database_batch(batch)
    
    def _process_database_batch(self, batch: List[BatchOperation]) -> None:
        """Process a batch of database operations."""
        if not batch:
            return
            
        start_time = time.time()
        print(f"🗄️  Processing database batch: {len(batch)} operations")
        
        # Group operations by collection and type
        from collections import defaultdict
        grouped_ops = defaultdict(lambda: defaultdict(list))
        
        for operation in batch:
            collection = operation.data['collection']
            op_type = operation.data['operation_type']
            grouped_ops[collection][op_type].append(operation)
        
        # Process each group
        for collection_name, operations_by_type in grouped_ops.items():
            collection = getattr(self.db, collection_name)
            
            for op_type, operations in operations_by_type.items():
                try:
                    if op_type == 'insert':
                        self._batch_insert(collection, operations)
                    elif op_type == 'update':
                        self._batch_update(collection, operations)
                    # Add more operation types as needed
                        
                except Exception as e:
                    print(f"❌ Batch {op_type} error for {collection_name}: {e}")
                    for operation in operations:
                        self._stats['failed_operations'] += 1
                        if operation.callback:
                            operation.callback(False, operation.id)
        
        # Update statistics
        batch_time = time.time() - start_time
        self._stats['batches_processed'] += 1
        self._stats['avg_batch_time'] = (
            (self._stats['avg_batch_time'] * (self._stats['batches_processed'] - 1) + batch_time) 
            / self._stats['batches_processed']
        )
        
        print(f"✅ Database batch completed in {batch_time:.2f}s")
    
    def _batch_insert(self, collection, operations: List[BatchOperation]) -> None:
        """Perform batch insert operations."""
        documents = [op.data['data'] for op in operations]
        result = collection.insert_many(documents, ordered=False)
        
        # Handle callbacks
        for operation in operations:
            self._stats['successful_operations'] += 1
            self._stats['total_processed'] += 1
            if operation.callback:
                operation.callback(True, operation.id)
    
    def _batch_update(self, collection, operations: List[BatchOperation]) -> None:
        """Perform batch update operations."""
        from pymongo import UpdateOne
        
        bulk_ops = []
        for operation in operations:
            data = operation.data['data']
            bulk_ops.append(UpdateOne(
                data['filter'],
                data['update'],
                upsert=data.get('upsert', False)
            ))
        
        result = collection.bulk_write(bulk_ops, ordered=False)
        
        # Handle callbacks
        for operation in operations:
            self._stats['successful_operations'] += 1
            self._stats['total_processed'] += 1
            if operation.callback:
                operation.callback(True, operation.id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get batch database statistics."""
        return {
            **self._stats,
            'queue_size': self._db_queue.qsize(),
            'success_rate': (
                self._stats['successful_operations'] / max(1, self._stats['total_processed'])
            ) if self._stats['total_processed'] > 0 else 0.0
        }
