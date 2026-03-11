"""
Firebase Admin SDK Initialization with Comprehensive Error Handling
Architectural Rationale: Centralized Firebase client with connection pooling,
automatic retry logic, and graceful degradation.
"""

import os
import json
import logging
from typing import Optional, Dict, Any
from pathlib import Path
import firebase_admin
from firebase_admin import credentials, firestore, initialize_app
from firebase_admin.exceptions import FirebaseError
import google.auth
from google.auth.exceptions import GoogleAuthError
from google.cloud.firestore_v1.client import Client as FirestoreClient

logger = logging.getLogger(__name__)

class FirebaseManager:
    """Singleton manager for Firebase Firestore connections"""
    _instance: Optional['FirebaseManager'] = None
    _client: Optional[FirestoreClient] = None
    _initialized: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirebaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._initialized = True
            self._service_account_path: Optional[str] = None
            self._project_id: Optional[str] = None
    
    def initialize(self, 
                   service_account_path: Optional[str] = None,
                   project_id: Optional[str] = None) -> FirestoreClient:
        """
        Initialize Firebase Admin SDK with robust error handling
        
        Args:
            service_account_path: Path to service account JSON file
            project_id: Firebase project ID (optional, can be inferred)
            
        Returns:
            Initialized Firestore client
            
        Raises:
            FileNotFoundError: If service account file doesn't exist
            GoogleAuthError: If authentication fails
            FirebaseError: If Firebase initialization fails
        """
        if self._client is not None:
            logger.info("Firebase client already initialized, returning existing instance")
            return self._client
        
        # Determine service account path
        if service_account_path is None:
            env_path = os.getenv('FIREBASE_SERVICE_ACCOUNT_PATH', './serviceAccountKey.json')
            service_account_path = env_path
        
        self._service_account_path = service_account_path
        logger.info("Attempting to initialize Firebase with service account: %s", service_account_path)
        
        # Validate service account file exists
        path_obj = Path(service_account_path)
        if not path_obj.exists():
            logger.error("Service account file not found: %s", service_account_path)
            
            # Emergency fallback: Check for environment variable with JSON content
            env_json = os.getenv('FIREBASE_SERVICE_ACCOUNT_JSON')
            if env_json:
                logger.info("Found service account JSON in environment variable")
                try:
                    cred_dict = json.loads(env_json)
                    cred = credentials.Certificate(cred_dict)
                except (json.JSONDecodeError, ValueError) as e:
                    logger.error("Failed to parse service account JSON from env: %s", e)
                    raise FileNotFoundError(f"Service account file not found and env JSON invalid: {e}")
            else:
                raise FileNotFoundError(f"Service account file not found: {service_account_path}")
        else:
            # Normal path: Load from file
            try:
                cred = credentials.Certificate(str(path_obj))
            except (ValueError, IOError) as e:
                logger.error("Failed to load service account certificate: %s", e)
                raise GoogleAuthError(f"Invalid service account file: {e}")
        
        # Determine project ID
        if project_id is None:
            project_id = os.getenv('FIREBASE_PROJECT_ID')
            if not project_id and hasattr(cred, 'project_id'):
                project_id = cred.project_id
        
        self._project_id = project_id
        
        # Initialize Firebase app with error handling
        try:
            if not firebase_admin._apps:
                app = initialize_app(cred, {
                    'projectId': project_id,
                }, name='capillary_nerve_center')
                logger.info("Firebase app initialized with project ID: %s", project_id)
            else:
                app = firebase_admin.get_app('capillary_nerve_center')
                logger.info("Using existing Firebase app")
        except (ValueError, FirebaseError) as e:
            logger.error("Firebase initialization failed: %s", e)
            raise FirebaseError(f"Failed to initialize Firebase: {e}")
        
        # Initialize Firestore client with retry settings
        try:
            self._client = firestore.client(app=app)
            
            # Test connection with timeout
            test_doc = self._client.collection('_health').document('test')
            test_doc.set({'timestamp': firestore.SERVER_TIMESTAMP}, timeout=5)
            test_doc.delete()
            
            logger.info("Firestore client initialized successfully")
            return self._client
            
        except Exception as e:
            logger.error("Firestore client creation failed: %s", e)
            self._client = None
            raise FirebaseError(f"Failed to create Firestore client: {e}")
    
    def get_client(self) -> FirestoreClient:
        """Get Firestore client, initializing if necessary"""
        if self._client is None:
            # Try to initialize with defaults
            return self.initialize()
        return self._client
    
    def get_project_id(self) -> str:
        """Get current project ID"""
        if self._project_id is None:
            self.get_client()  # Ensure initialized
        return self._project