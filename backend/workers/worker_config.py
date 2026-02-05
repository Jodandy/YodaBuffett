#!/usr/bin/env python3
"""
Worker Configuration Management

Centralized configuration for all worker services with environment
variable support and production-ready defaults.

Features:
- Environment-based configuration
- Production vs development settings
- Docker-friendly defaults
- Validation and type safety
"""

import os
from dataclasses import dataclass
from typing import Optional, List, Dict
from enum import Enum
import logging

class WorkerMode(Enum):
    """Worker execution modes"""
    DEVELOPMENT = "development"
    PRODUCTION = "production"
    TEST = "test"

class LogLevel(Enum):
    """Logging levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "yodabuffett"
    username: str = "postgres"
    password: str = ""
    
    @property
    def connection_url(self) -> str:
        """Generate async PostgreSQL connection URL"""
        if self.password:
            return f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            return f"postgresql+asyncpg://{self.username}@{self.host}:{self.port}/{self.database}"

@dataclass
class SchedulerConfig:
    """Event scheduler configuration"""
    look_ahead_days: int = 3
    look_back_days: int = 1
    weekly_sample_size: int = 50
    max_daily_targets: int = 100
    
@dataclass
class ScrapingConfig:
    """Scraping behavior configuration"""
    rate_limit_delay: float = 2.0  # Seconds between requests
    request_timeout: int = 30      # Request timeout in seconds
    max_retries: int = 3           # Max retry attempts
    retry_delay: float = 5.0       # Delay between retries
    user_agent: str = "YodaBuffett-Worker/1.0 (+https://github.com/user/yodabuffett)"
    
@dataclass
class WorkerConfig:
    """Complete worker configuration"""
    mode: WorkerMode = WorkerMode.DEVELOPMENT
    log_level: LogLevel = LogLevel.INFO
    
    # Component configurations
    database: DatabaseConfig = None
    scheduler: SchedulerConfig = None
    scraping: ScrapingConfig = None
    
    # Worker-specific settings
    worker_name: str = "daily-event-worker"
    run_interval_hours: int = 24    # How often to run daily worker
    health_check_port: int = 8080   # Health check endpoint port
    
    # Docker and deployment
    data_volume_path: str = "/app/data"
    log_file_path: Optional[str] = None
    
    def __post_init__(self):
        """Initialize sub-configurations"""
        if self.database is None:
            self.database = DatabaseConfig()
        if self.scheduler is None:
            self.scheduler = SchedulerConfig()
        if self.scraping is None:
            self.scraping = ScrapingConfig()

class ConfigManager:
    """Manages worker configuration with environment variable support"""
    
    @staticmethod
    def load_from_environment() -> WorkerConfig:
        """Load configuration from environment variables"""
        
        # Main worker settings
        mode = WorkerMode(os.getenv('WORKER_MODE', 'development'))
        log_level = LogLevel(os.getenv('LOG_LEVEL', 'INFO'))
        worker_name = os.getenv('WORKER_NAME', 'daily-event-worker')
        
        # Database configuration
        db_config = DatabaseConfig(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'yodabuffett'),
            username=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', '')
        )
        
        # Scheduler configuration
        scheduler_config = SchedulerConfig(
            look_ahead_days=int(os.getenv('SCHEDULER_LOOK_AHEAD_DAYS', '3')),
            look_back_days=int(os.getenv('SCHEDULER_LOOK_BACK_DAYS', '1')),
            weekly_sample_size=int(os.getenv('WEEKLY_SAMPLE_SIZE', '50')),
            max_daily_targets=int(os.getenv('MAX_DAILY_TARGETS', '100'))
        )
        
        # Scraping configuration
        scraping_config = ScrapingConfig(
            rate_limit_delay=float(os.getenv('SCRAPING_RATE_DELAY', '2.0')),
            request_timeout=int(os.getenv('SCRAPING_TIMEOUT', '30')),
            max_retries=int(os.getenv('SCRAPING_MAX_RETRIES', '3')),
            retry_delay=float(os.getenv('SCRAPING_RETRY_DELAY', '5.0')),
            user_agent=os.getenv('USER_AGENT', 'YodaBuffett-Worker/1.0')
        )
        
        # Worker operational settings
        run_interval = int(os.getenv('RUN_INTERVAL_HOURS', '24'))
        health_port = int(os.getenv('HEALTH_CHECK_PORT', '8080'))
        data_path = os.getenv('DATA_VOLUME_PATH', '/app/data')
        log_path = os.getenv('LOG_FILE_PATH', None)
        
        config = WorkerConfig(
            mode=mode,
            log_level=log_level,
            database=db_config,
            scheduler=scheduler_config,
            scraping=scraping_config,
            worker_name=worker_name,
            run_interval_hours=run_interval,
            health_check_port=health_port,
            data_volume_path=data_path,
            log_file_path=log_path
        )
        
        return config
    
    @staticmethod
    def setup_logging(config: WorkerConfig):
        """Configure logging based on configuration"""
        
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        if config.log_file_path:
            # Log to file in production
            logging.basicConfig(
                level=getattr(logging, config.log_level.value),
                format=log_format,
                filename=config.log_file_path,
                filemode='a'
            )
        else:
            # Log to console for development/Docker
            logging.basicConfig(
                level=getattr(logging, config.log_level.value),
                format=log_format
            )
        
        # Reduce SQLAlchemy noise in production
        if config.mode == WorkerMode.PRODUCTION:
            logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
            logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
        
        logger = logging.getLogger(__name__)
        logger.info(f"🔧 Configuration loaded - Mode: {config.mode.value} | Log Level: {config.log_level.value}")
        
        return logger
    
    @staticmethod
    def validate_config(config: WorkerConfig) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        # Database validation
        if not config.database.host:
            errors.append("Database host is required")
        if not config.database.database:
            errors.append("Database name is required")
        if not config.database.username:
            errors.append("Database username is required")
            
        # Scheduler validation
        if config.scheduler.look_ahead_days < 1:
            errors.append("Look ahead days must be at least 1")
        if config.scheduler.weekly_sample_size < 1:
            errors.append("Weekly sample size must be at least 1")
            
        # Scraping validation
        if config.scraping.rate_limit_delay < 0.1:
            errors.append("Rate limit delay must be at least 0.1 seconds")
        if config.scraping.request_timeout < 1:
            errors.append("Request timeout must be at least 1 second")
            
        return errors

# Global configuration instance
_config = None

def get_config() -> WorkerConfig:
    """Get the global configuration instance"""
    global _config
    if _config is None:
        _config = ConfigManager.load_from_environment()
        
        # Validate configuration
        errors = ConfigManager.validate_config(_config)
        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")
            
    return _config

def setup_worker_logging() -> logging.Logger:
    """Setup logging for worker and return logger"""
    config = get_config()
    return ConfigManager.setup_logging(config)

# Example environment variables documentation
EXAMPLE_ENV_VARS = {
    "WORKER_MODE": "production",
    "LOG_LEVEL": "INFO",
    "WORKER_NAME": "daily-event-worker",
    
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_NAME": "yodabuffett",
    "DB_USER": "yodabuffett",
    "DB_PASSWORD": "password",
    
    "SCHEDULER_LOOK_AHEAD_DAYS": "3",
    "SCHEDULER_LOOK_BACK_DAYS": "1",
    "WEEKLY_SAMPLE_SIZE": "50",
    "MAX_DAILY_TARGETS": "100",
    
    "SCRAPING_RATE_DELAY": "2.0",
    "SCRAPING_TIMEOUT": "30",
    "SCRAPING_MAX_RETRIES": "3",
    "USER_AGENT": "YodaBuffett-Worker/1.0",
    
    "RUN_INTERVAL_HOURS": "24",
    "HEALTH_CHECK_PORT": "8080",
    "DATA_VOLUME_PATH": "/app/data",
    "LOG_FILE_PATH": "/app/logs/worker.log"
}

if __name__ == "__main__":
    """Test configuration loading"""
    print("🔧 Worker Configuration Test")
    print("=" * 40)
    
    config = get_config()
    
    print(f"Mode: {config.mode.value}")
    print(f"Log Level: {config.log_level.value}")
    print(f"Worker Name: {config.worker_name}")
    print(f"Database: {config.database.connection_url}")
    print(f"Scheduler Look Ahead: {config.scheduler.look_ahead_days} days")
    print(f"Rate Limit: {config.scraping.rate_limit_delay}s")
    
    # Test validation
    errors = ConfigManager.validate_config(config)
    if errors:
        print(f"\n❌ Configuration Errors:")
        for error in errors:
            print(f"  - {error}")
    else:
        print(f"\n✅ Configuration validation passed!")
    
    print(f"\n📋 Example environment variables:")
    for key, value in EXAMPLE_ENV_VARS.items():
        print(f"  {key}={value}")