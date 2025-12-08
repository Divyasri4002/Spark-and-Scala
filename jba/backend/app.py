from flask import Flask, jsonify, render_template
from flask_cors import CORS
from spark_jobs import JobMarketAnalyzer
import sys
import config
import json

# Serve templates and static assets from the project's `frontend` folder
# Set `static_url_path` to empty so files under `frontend` are served from '/'
app = Flask(__name__, template_folder="../frontend", static_folder="../frontend", static_url_path="")
CORS(app)

# Initialize Spark analyzer and defer loading the dataset until needed
analyzer = JobMarketAnalyzer()
df = None

def get_df():
    """Lazily load and cache the DataFrame. Raises FileNotFoundError if dataset missing."""
    global df
    if df is None:
        try:
            df = analyzer.load_data(config.Config.DATA_PATH)
        except FileNotFoundError as e:
            # Re-raise so endpoints can handle or the app can log the issue
            raise
    return df

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/domain-analysis')
def domain_analysis():
    df_local = get_df()
    result = analyzer.get_domain_analysis(df_local)
    return jsonify(result)

@app.route('/api/skill-demand')
def skill_demand():
    df_local = get_df()
    result = analyzer.get_skill_demand(df_local)
    return jsonify(result)

@app.route('/api/company-analysis')
def company_analysis():
    df_local = get_df()
    result = analyzer.get_company_analysis(df_local)
    return jsonify(result)

@app.route('/api/location-analysis')
def location_analysis():
    df_local = get_df()
    result = analyzer.get_location_analysis(df_local)
    return jsonify(result)

@app.route('/api/experience-trends')
def experience_trends():
    df_local = get_df()
    result = analyzer.get_experience_trends(df_local)
    return jsonify(result)

@app.route('/api/salary-distribution')
def salary_distribution():
    df_local = get_df()
    result = analyzer.get_salary_distribution(df_local)
    return jsonify(result)

@app.route('/api/dataset-info')
def dataset_info():
    # Basic dataset information
    df_local = get_df()
    total_jobs = df_local.count()
    domains = df_local.select("domain").distinct().count()
    companies = df_local.select("company").distinct().count()
    locations = df_local.select("city").distinct().count()
    
    info = {
        "total_jobs": total_jobs,
        "unique_domains": domains,
        "unique_companies": companies,
        "unique_locations": locations,
        "spark_ui_url": f"http://localhost:{config.Config.SPARK_UI_PORT}"
    }
    return jsonify(info)

if __name__ == '__main__':
    print(f"Spark UI available at: http://localhost:{config.Config.SPARK_UI_PORT}")
    print(f"Flask App available at: http://localhost:{config.Config.FLASK_PORT}")
    app.run(debug=True, port=config.Config.FLASK_PORT)