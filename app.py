#!/usr/bin/env python3
"""
Flask web application for News Sentiment Dashboard
Serves HTML dashboards and provides API endpoints to run data collection scripts
"""

from flask import Flask, send_from_directory, jsonify, request
import os
import sys
from datetime import datetime
import subprocess
import threading
from pathlib import Path

# Add scripts directory to path
sys.path.append(str(Path(__file__).parent / 'scripts'))

app = Flask(__name__, static_folder='.')

# Configuration
PORT = int(os.environ.get('PORT', 8080))
BUCKET_NAME = os.environ.get('GCS_BUCKET', 'risk-dashboard')  # Default to risk-dashboard

# Track running jobs
active_jobs = {}


@app.route('/')
def index():
    """Serve the main dashboard page"""
    return send_from_directory('.', 'dashboard.html')


@app.route('/<path:path>')
def serve_file(path):
    """Serve static files (HTML, CSS, JS, etc.)"""
    try:
        return send_from_directory('.', path)
    except Exception as e:
        return jsonify({'error': str(e)}), 404


@app.route('/health')
def health():
    """Health check endpoint for Cloud Run"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'bucket': BUCKET_NAME
    })


@app.route('/api/status')
def status():
    """Get status of data collection jobs"""
    return jsonify({
        'active_jobs': list(active_jobs.keys()),
        'job_details': active_jobs,
        'bucket': BUCKET_NAME,
        'timestamp': datetime.utcnow().isoformat()
    })


def run_script_background(script_name, args=None):
    """Run a Python script in the background"""
    job_id = f"{script_name}_{datetime.utcnow().isoformat()}"
    
    def run():
        active_jobs[job_id] = {
            'script': script_name,
            'status': 'running',
            'started': datetime.utcnow().isoformat()
        }
        
        try:
            cmd = ['python', f'scripts/{script_name}.py']
            if args:
                cmd.extend(args)
            if BUCKET_NAME:
                cmd.extend(['--bucket', BUCKET_NAME])
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)  # 30 min timeout
            
            active_jobs[job_id].update({
                'status': 'completed' if result.returncode == 0 else 'failed',
                'completed': datetime.utcnow().isoformat(),
                'returncode': result.returncode,
                'stdout': result.stdout[-2000:],  # Last 2000 chars
                'stderr': result.stderr[-2000:] if result.stderr else None
            })
            
        except subprocess.TimeoutExpired:
            active_jobs[job_id].update({
                'status': 'timeout',
                'completed': datetime.utcnow().isoformat()
            })
        except Exception as e:
            active_jobs[job_id].update({
                'status': 'error',
                'completed': datetime.utcnow().isoformat(),
                'error': str(e)
            })
    
    thread = threading.Thread(target=run)
    thread.start()
    
    return job_id


# =============================================================================
# DEBUG ENDPOINTS
# =============================================================================

@app.route('/api/debug/config')
def debug_config():
    """Debug endpoint to check configuration"""
    import shutil
    
    app_dir = Path(__file__).parent
    scripts_dir = app_dir / 'scripts'
    
    # Check if scripts exist (in /app/scripts/)
    scripts = [
        'news_articles_brands.py',
        'news_articles_ceos.py', 
        'fetch_stock_data.py',
    ]
    script_status = {s: (scripts_dir / s).exists() for s in scripts}
    
    # Check storage_utils.py at root level (/app/storage_utils.py)
    storage_utils_exists = (app_dir / 'storage_utils.py').exists()
    
    # List what's actually in scripts dir
    scripts_dir_contents = []
    if scripts_dir.exists():
        scripts_dir_contents = [f.name for f in scripts_dir.iterdir() if f.is_file()][:20]
    
    # List what's in app root
    app_dir_contents = [f.name for f in app_dir.iterdir() if f.is_file()][:20]
    
    # Check Python
    python_path = shutil.which('python') or shutil.which('python3')
    
    return jsonify({
        'bucket_name': BUCKET_NAME,
        'bucket_from_env': os.environ.get('GCS_BUCKET'),
        'port': PORT,
        'app_dir': str(app_dir),
        'app_dir_files': app_dir_contents,
        'scripts_dir': str(scripts_dir),
        'scripts_dir_files': scripts_dir_contents,
        'scripts_exist': script_status,
        'storage_utils_exists': storage_utils_exists,
        'storage_utils_path': str(app_dir / 'storage_utils.py'),
        'python_path': python_path,
        'cwd': os.getcwd(),
        'google_credentials': bool(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')),
    })


@app.route('/api/debug/test-gcs')
def test_gcs():
    """Test GCS connectivity"""
    try:
        # Add parent to path if needed
        app_dir = Path(__file__).parent
        if str(app_dir) not in sys.path:
            sys.path.insert(0, str(app_dir))
        
        from storage_utils import CloudStorageManager
        storage = CloudStorageManager(BUCKET_NAME)
        
        # Try to list files
        files = storage.list_files('rosters/')
        roster_exists = storage.file_exists('rosters/main-roster.csv')
        
        # Also check data directories
        data_files = storage.list_files('data/')[:10]
        
        return jsonify({
            'status': 'success',
            'bucket': BUCKET_NAME,
            'roster_exists': roster_exists,
            'roster_files': files[:10] if files else [],
            'data_files': data_files,
            'can_connect': True
        })
    except Exception as e:
        import traceback
        return jsonify({
            'status': 'error',
            'bucket': BUCKET_NAME,
            'error': str(e),
            'error_type': type(e).__name__,
            'traceback': traceback.format_exc(),
            'can_connect': False
        }), 500


@app.route('/api/debug/test-script')
def test_script():
    """Test running a simple script to verify subprocess works"""
    try:
        # Run a simple Python command
        result = subprocess.run(
            ['python', '-c', 'import sys; print(sys.path); print("OK")'],
            capture_output=True, 
            text=True, 
            timeout=30,
            cwd=str(Path(__file__).parent)
        )
        
        return jsonify({
            'status': 'success' if result.returncode == 0 else 'failed',
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


@app.route('/api/debug/run-sync')
def run_sync_test():
    """
    Run news_articles_brands synchronously with small batch for testing.
    """
    try:
        cmd = [
            'python', 'scripts/news_articles_brands.py',
            '--bucket', BUCKET_NAME,
            '--batch-size', '5',
            '--no-resume'
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(Path(__file__).parent)
        )
        
        return jsonify({
            'status': 'completed' if result.returncode == 0 else 'failed',
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'command': ' '.join(cmd)
        })
        
    except subprocess.TimeoutExpired:
        return jsonify({
            'status': 'timeout',
            'error': 'Script took longer than 120 seconds'
        }), 500
    except Exception as e:
        import traceback
        return jsonify({
            'status': 'error',
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


@app.route('/api/debug/test-script-help')
def test_script_help():
    """Test if the script can even start by running --help"""
    try:
        result = subprocess.run(
            ['python', 'scripts/news_articles_brands.py', '--help'],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(Path(__file__).parent)
        )
        
        return jsonify({
            'status': 'ok' if result.returncode == 0 else 'failed',
            'returncode': result.returncode,
            'stdout': result.stdout[:2000],
            'stderr': result.stderr[:2000] if result.stderr else None
        })
    except subprocess.TimeoutExpired:
        return jsonify({'status': 'timeout', 'error': 'Script --help took > 30s'}), 500
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/api/debug/test-script-import')
def test_script_import():
    """Test if the script can import all its dependencies"""
    try:
        # Run Python to just import the script (not execute it)
        code = """
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/app/scripts')
print("Starting imports...")
import argparse
print("  argparse OK")
import json, os, time, urllib.parse
print("  stdlib OK")
from datetime import datetime, timezone
print("  datetime OK")
from pathlib import Path
print("  pathlib OK")
import pandas as pd
print("  pandas OK")
import requests
print("  requests OK")
from requests.adapters import HTTPAdapter
print("  HTTPAdapter OK")
from urllib3.util.retry import Retry
print("  Retry OK")
from bs4 import BeautifulSoup
print("  BeautifulSoup OK")
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
print("  VADER OK")
from storage_utils import CloudStorageManager
print("  CloudStorageManager OK")
print("All imports successful!")
"""
        result = subprocess.run(
            ['python', '-c', code],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(Path(__file__).parent)
        )
        
        return jsonify({
            'status': 'ok' if result.returncode == 0 else 'failed',
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr if result.stderr else None
        })
    except subprocess.TimeoutExpired:
        return jsonify({'status': 'timeout', 'error': 'Imports took > 60s'}), 500
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/api/debug/mini-run')
def mini_run():
    """
    Minimal end-to-end test: fetch 3 companies, save to GCS.
    Does the actual work INLINE (not via subprocess).
    """
    import time
    import urllib.parse
    import requests
    from bs4 import BeautifulSoup
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    import pandas as pd
    
    results = {'steps': [], 'articles': []}
    start_total = time.time()
    
    try:
        # Step 1: Setup
        from storage_utils import CloudStorageManager
        storage = CloudStorageManager(BUCKET_NAME)
        analyzer = SentimentIntensityAnalyzer()
        results['steps'].append({'step': 'setup', 'status': 'ok'})
        
        # Step 2: Load roster and get first 3 companies
        df = storage.read_csv('rosters/main-roster.csv')
        df.columns = [c.strip().lower() for c in df.columns]
        companies = df['company'].dropna().astype(str).str.strip().tolist()[:3]
        results['steps'].append({'step': 'load_roster', 'companies': companies})
        
        # Step 3: Fetch articles for each company
        all_rows = []
        for company in companies:
            step_start = time.time()
            try:
                query = urllib.parse.quote(f'"{company}"')
                url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
                
                resp = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
                resp.raise_for_status()
                
                soup = BeautifulSoup(resp.text, 'xml')
                items = soup.find_all('item')[:5]  # Max 5 per company
                
                for item in items:
                    title = (item.title.text or '').strip() if item.title else ''
                    link = (item.link.text or '').strip() if item.link else ''
                    
                    # Sentiment
                    scores = analyzer.polarity_scores(title)
                    compound = scores['compound']
                    if compound >= 0.25:
                        sentiment = 'positive'
                    elif compound <= -0.05:
                        sentiment = 'negative'
                    else:
                        sentiment = 'neutral'
                    
                    all_rows.append({
                        'company': company,
                        'title': title[:100],
                        'url': link,
                        'sentiment': sentiment,
                        'date': datetime.utcnow().strftime('%Y-%m-%d')
                    })
                
                results['steps'].append({
                    'step': f'fetch_{company}',
                    'articles': len(items),
                    'time': round(time.time() - step_start, 2)
                })
                
            except Exception as e:
                results['steps'].append({
                    'step': f'fetch_{company}',
                    'error': str(e)
                })
        
        # Step 4: Save to GCS
        if all_rows:
            output_df = pd.DataFrame(all_rows)
            today = datetime.utcnow().strftime('%Y-%m-%d')
            output_path = f'data/processed_articles/{today}-brand-articles-TEST.csv'
            storage.write_csv(output_df, output_path, index=False)
            results['steps'].append({
                'step': 'save_to_gcs',
                'path': output_path,
                'rows': len(output_df)
            })
            results['articles'] = all_rows[:10]  # Return first 10 for inspection
        
        results['total_time'] = round(time.time() - start_total, 2)
        results['status'] = 'success'
        return jsonify(results)
        
    except Exception as e:
        import traceback
        results['status'] = 'error'
        results['error'] = str(e)
        results['traceback'] = traceback.format_exc()
        return jsonify(results), 500


@app.route('/api/debug/step-by-step')
def step_by_step_test():
    """
    Step-by-step test to isolate exactly where the script hangs.
    """
    import time
    results = {}
    
    # Step 1: Import storage_utils
    try:
        start = time.time()
        from storage_utils import CloudStorageManager
        results['step1_import'] = {'status': 'ok', 'time': round(time.time() - start, 2)}
    except Exception as e:
        results['step1_import'] = {'status': 'failed', 'error': str(e)}
        return jsonify(results), 500
    
    # Step 2: Create storage manager
    try:
        start = time.time()
        storage = CloudStorageManager(BUCKET_NAME)
        results['step2_storage_init'] = {'status': 'ok', 'time': round(time.time() - start, 2)}
    except Exception as e:
        results['step2_storage_init'] = {'status': 'failed', 'error': str(e)}
        return jsonify(results), 500
    
    # Step 3: Check roster exists
    try:
        start = time.time()
        exists = storage.file_exists('rosters/main-roster.csv')
        results['step3_roster_exists'] = {'status': 'ok', 'exists': exists, 'time': round(time.time() - start, 2)}
    except Exception as e:
        results['step3_roster_exists'] = {'status': 'failed', 'error': str(e)}
        return jsonify(results), 500
    
    # Step 4: Read roster
    try:
        start = time.time()
        import pandas as pd
        df = storage.read_csv('rosters/main-roster.csv')
        results['step4_read_roster'] = {'status': 'ok', 'rows': len(df), 'time': round(time.time() - start, 2)}
    except Exception as e:
        results['step4_read_roster'] = {'status': 'failed', 'error': str(e)}
        return jsonify(results), 500
    
    # Step 5: Test HTTP request to Google News
    try:
        start = time.time()
        import requests
        url = "https://news.google.com/rss/search?q=Apple&hl=en-US&gl=US&ceid=US:en"
        resp = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        results['step5_google_news'] = {
            'status': 'ok', 
            'http_status': resp.status_code,
            'content_length': len(resp.content),
            'time': round(time.time() - start, 2)
        }
    except Exception as e:
        results['step5_google_news'] = {'status': 'failed', 'error': str(e)}
        return jsonify(results), 500
    
    # Step 6: Test writing to GCS
    try:
        start = time.time()
        test_path = 'data/test/connectivity-test.txt'
        storage.write_text(f'Test at {datetime.utcnow().isoformat()}', test_path)
        results['step6_write_gcs'] = {'status': 'ok', 'path': test_path, 'time': round(time.time() - start, 2)}
    except Exception as e:
        results['step6_write_gcs'] = {'status': 'failed', 'error': str(e)}
        return jsonify(results), 500
    
    # Step 7: Quick VADER test
    try:
        start = time.time()
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        analyzer = SentimentIntensityAnalyzer()
        score = analyzer.polarity_scores("This is a test headline")
        results['step7_vader'] = {'status': 'ok', 'score': score, 'time': round(time.time() - start, 2)}
    except Exception as e:
        results['step7_vader'] = {'status': 'failed', 'error': str(e)}
        return jsonify(results), 500
    
    results['overall'] = 'All steps passed!'
    return jsonify(results)


@app.route('/api/debug/test-import')
def test_import():
    """Test importing storage_utils from scripts directory context"""
    try:
        # Simulate what the scripts do
        scripts_dir = Path(__file__).parent / 'scripts'
        parent_dir = scripts_dir.parent  # This is /app
        
        # Add parent to path like scripts do
        if str(parent_dir) not in sys.path:
            sys.path.insert(0, str(parent_dir))
        
        # Try to import
        from storage_utils import CloudStorageManager
        
        # Try to instantiate
        storage = CloudStorageManager(BUCKET_NAME)
        
        return jsonify({
            'status': 'success',
            'bucket_name': storage.bucket_name,
            'sys_path': sys.path[:5],
            'parent_dir': str(parent_dir),
            'storage_utils_location': str(parent_dir / 'storage_utils.py'),
            'exists': (parent_dir / 'storage_utils.py').exists()
        })
    except Exception as e:
        import traceback
        return jsonify({
            'status': 'error',
            'error': str(e),
            'error_type': type(e).__name__,
            'traceback': traceback.format_exc(),
            'sys_path': sys.path[:5]
        }), 500


# =============================================================================
# DATA COLLECTION ENDPOINTS
# =============================================================================

@app.route('/api/collect/stock-data', methods=['POST'])
def collect_stock_data():
    """Trigger stock data collection"""
    if not BUCKET_NAME:
        return jsonify({'error': 'GCS_BUCKET not configured'}), 500
    
    job_id = run_script_background('fetch_stock_data')
    
    return jsonify({
        'message': 'Stock data collection started',
        'job_id': job_id,
        'status_url': f'/api/status'
    })


@app.route('/api/collect/trends-data', methods=['POST'])
def collect_trends_data():
    """Trigger Google Trends data collection"""
    if not BUCKET_NAME:
        return jsonify({'error': 'GCS_BUCKET not configured'}), 500
    
    job_id = run_script_background('fetch_trends_data')
    
    return jsonify({
        'message': 'Trends data collection started',
        'job_id': job_id,
        'status_url': f'/api/status'
    })


@app.route('/api/collect/news-brands', methods=['POST'])
def collect_news_brands():
    """Trigger brand news collection"""
    if not BUCKET_NAME:
        return jsonify({'error': 'GCS_BUCKET not configured'}), 500
    
    date = request.args.get('date')
    args = ['--date', date] if date else []
    
    job_id = run_script_background('news_articles_brands', args)
    
    return jsonify({
        'message': 'Brand news collection started',
        'job_id': job_id,
        'date': date or 'today',
        'status_url': f'/api/status'
    })


@app.route('/api/collect/news-ceos', methods=['POST'])
def collect_news_ceos():
    """Trigger CEO news collection"""
    if not BUCKET_NAME:
        return jsonify({'error': 'GCS_BUCKET not configured'}), 500
    
    date = request.args.get('date')
    args = ['--date', date] if date else []
    
    job_id = run_script_background('news_articles_ceos', args)
    
    return jsonify({
        'message': 'CEO news collection started',
        'job_id': job_id,
        'date': date or 'today',
        'status_url': f'/api/status'
    })


@app.route('/api/collect/aggregate-negative', methods=['POST'])
def aggregate_negative():
    """Trigger negative articles aggregation"""
    if not BUCKET_NAME:
        return jsonify({'error': 'GCS_BUCKET not configured'}), 500
    
    days_back = request.args.get('days_back', '90')
    args = ['--days-back', days_back]
    
    job_id = run_script_background('aggregate_negative_articles', args)
    
    return jsonify({
        'message': 'Negative articles aggregation started',
        'job_id': job_id,
        'days_back': days_back,
        'status_url': f'/api/status'
    })


@app.route('/api/collect/process-serps-brands', methods=['POST'])
def process_serps_brands():
    """Trigger brand SERP processing"""
    if not BUCKET_NAME:
        return jsonify({'error': 'GCS_BUCKET not configured'}), 500
    
    date = request.args.get('date')
    args = ['--date', date] if date else []
    
    job_id = run_script_background('process_serps_brands', args)
    
    return jsonify({
        'message': 'Brand SERP processing started',
        'job_id': job_id,
        'date': date or 'today',
        'status_url': f'/api/status'
    })


@app.route('/api/collect/process-serps-ceos', methods=['POST'])
def process_serps_ceos():
    """Trigger CEO SERP processing"""
    if not BUCKET_NAME:
        return jsonify({'error': 'GCS_BUCKET not configured'}), 500
    
    date = request.args.get('date')
    args = ['--date', date] if date else []
    
    job_id = run_script_background('process_serps_ceos', args)
    
    return jsonify({
        'message': 'CEO SERP processing started',
        'job_id': job_id,
        'date': date or 'today',
        'status_url': f'/api/status'
    })


@app.route('/api/collect/sentiment-brands', methods=['POST'])
def sentiment_brands():
    """Trigger brand sentiment aggregation (runs after news-brands)"""
    if not BUCKET_NAME:
        return jsonify({'error': 'GCS_BUCKET not configured'}), 500
    
    date = request.args.get('date')
    args = ['--date', date] if date else []
    
    job_id = run_script_background('news_sentiment_brands', args)
    
    return jsonify({
        'message': 'Brand sentiment aggregation started',
        'job_id': job_id,
        'date': date or 'today',
        'status_url': f'/api/status'
    })


@app.route('/api/collect/sentiment-ceos', methods=['POST'])
def sentiment_ceos():
    """Trigger CEO sentiment aggregation (runs after news-ceos)"""
    if not BUCKET_NAME:
        return jsonify({'error': 'GCS_BUCKET not configured'}), 500
    
    date = request.args.get('date')
    args = ['--date', date] if date else []
    
    job_id = run_script_background('news_sentiment_ceos', args)
    
    return jsonify({
        'message': 'CEO sentiment aggregation started',
        'job_id': job_id,
        'date': date or 'today',
        'status_url': f'/api/status'
    })


@app.route('/api/collect/all', methods=['POST'])
def collect_all():
    """Trigger full data collection pipeline (in proper sequence)"""
    if not BUCKET_NAME:
        return jsonify({'error': 'GCS_BUCKET not configured'}), 500
    
    jobs = {}
    
    # Run scripts in proper sequence (in background)
    # Phase 1: Fetch raw data
    jobs['stock_data'] = run_script_background('fetch_stock_data')
    jobs['trends_data'] = run_script_background('fetch_trends_data')
    jobs['brand_articles'] = run_script_background('news_articles_brands')
    jobs['ceo_articles'] = run_script_background('news_articles_ceos')
    
    # Phase 2: Aggregate sentiment (these need articles from Phase 1)
    # Note: These run in parallel with Phase 1 - might want to add delay in production
    jobs['brand_sentiment'] = run_script_background('news_sentiment_brands')
    jobs['ceo_sentiment'] = run_script_background('news_sentiment_ceos')
    
    # Phase 3: Process SERPs
    jobs['brand_serps'] = run_script_background('process_serps_brands')
    jobs['ceo_serps'] = run_script_background('process_serps_ceos')
    
    # Phase 4: Aggregate negative articles
    jobs['negative_summary'] = run_script_background('aggregate_negative_articles', ['--days-back', '90'])
    
    return jsonify({
        'message': 'Full data collection pipeline started',
        'job_ids': jobs,
        'note': 'Scripts run in parallel. Sentiment scripts need articles to be fetched first.',
        'status_url': f'/api/status'
    })


@app.route('/api/trigger/daily', methods=['POST'])
def trigger_daily():
    """
    Endpoint for Cloud Scheduler to trigger daily collection
    Runs complete data pipeline in proper sequence
    """
    if not BUCKET_NAME:
        return jsonify({'error': 'GCS_BUCKET not configured'}), 500
    
    jobs = {
        # Phase 1: Fetch raw data
        'stock_data': run_script_background('fetch_stock_data'),
        'trends_data': run_script_background('fetch_trends_data'),
        'brand_articles': run_script_background('news_articles_brands'),
        'ceo_articles': run_script_background('news_articles_ceos'),
        
        # Phase 2: Aggregate sentiment (needs articles from Phase 1)
        'brand_sentiment': run_script_background('news_sentiment_brands'),
        'ceo_sentiment': run_script_background('news_sentiment_ceos'),
        
        # Phase 3: Process SERPs
        'brand_serps': run_script_background('process_serps_brands'),
        'ceo_serps': run_script_background('process_serps_ceos'),
        
        # Phase 4: Aggregate negative articles
        'negative_summary': run_script_background('aggregate_negative_articles', ['--days-back', '90'])
    }
    
    return jsonify({
        'message': 'Daily collection triggered',
        'timestamp': datetime.utcnow().isoformat(),
        'jobs': jobs,
        'note': 'All scripts running. Check /api/status for progress.'
    })


if __name__ == '__main__':
    # Log configuration
    if os.environ.get('GCS_BUCKET'):
        print(f"☁️  Using Cloud Storage bucket (from env): {BUCKET_NAME}")
    else:
        print(f"☁️  Using Cloud Storage bucket (default): {BUCKET_NAME}")
    print()
    
    print(f"🚀 Starting News Sentiment Dashboard on port {PORT}")
    print(f"📊 Dashboard: http://localhost:{PORT}")
    print(f"🔧 API Status: http://localhost:{PORT}/api/status")
    print(f"💾 Health Check: http://localhost:{PORT}/health")
    print(f"🔍 Debug Config: http://localhost:{PORT}/api/debug/config")
    print(f"🔍 Test GCS: http://localhost:{PORT}/api/debug/test-gcs")
    print()
    
    # Run Flask app
    app.run(host='0.0.0.0', port=PORT, debug=False)
