# iris_ml_processor.py
import pandas as pd
import psycopg2
import psycopg2.extras
import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine
import os

def process_iris_data(**kwargs):
    """
    Process Iris dataset from PostgreSQL, train a model, and save results.
    This function is designed to be used with Airflow's PythonOperator.
    """
    # Get connection parameters from environment variables or use defaults
    pg_host = os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres_analytics')
    pg_port = os.getenv('POSTGRES_PORT', '5432')
    pg_db = os.getenv('ANALYTICS_DB', 'analytics')
    pg_user = os.getenv('ETL_USER', 'etl_user')
    pg_password = os.getenv('ETL_PASSWORD', 'etl_password')
    
    # Create SQLAlchemy engine for DataFrame operations
    conn_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(conn_string)
    
    # Query the processed Iris data from the dbt-transformed table
    query = """
    SELECT * FROM homework.iris_processed
    """
    
    df = pd.read_sql(query, engine)
    print(f"Loaded data: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # Only keep the label encoding of our target variable
    df.drop(
        [
            'species',
            'is_species__setosa',
            'is_species__versicolor',
            'is_species__virginica',
            'is_species__',
         ],
         axis=1, inplace=True, errors='ignore'  # Use errors='ignore' to handle columns that might not exist
    )
    
    X = df.drop(columns=['species_label_encoded'])
    y = df['species_label_encoded']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    print(f"Training set: {X_train.shape[0]} samples, Test set: {X_test.shape[0]} samples")
    
    # Train the initial model
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)
    
    # Get initial model performance
    train_score = clf.score(X_train, y_train)
    test_score = clf.score(X_test, y_test)
    print(f"Initial model - Training accuracy: {train_score:.4f}, Test accuracy: {test_score:.4f}")
    
    # Get feature importances
    importances = clf.feature_importances_
    feature_names = X_train.columns
    feature_importance_df = pd.DataFrame({
                              'Feature': feature_names,
                              'Importance': importances
                            })
    
    # Select the top 5 features
    top_features = feature_importance_df.sort_values(
                      by='Importance',
                      ascending=False
                    ).head(5)['Feature'].tolist()
    
    print(f"Top 5 features: {', '.join(top_features)}")
    
    # Filter training and testing sets based on the top 5 features
    X_train_top5 = X_train[top_features]
    X_test_top5 = X_test[top_features]
    
    # Train a new model with only the top 5 features
    clf_top5 = RandomForestClassifier(n_estimators=100, random_state=42)
    clf_top5.fit(X_train_top5, y_train)
    
    # Get top 5 features model performance
    train_score_top5 = clf_top5.score(X_train_top5, y_train)
    test_score_top5 = clf_top5.score(X_test_top5, y_test)
    print(f"Top 5 features model - Training accuracy: {train_score_top5:.4f}, Test accuracy: {test_score_top5:.4f}")
    
    # Save results to database
    results_df = pd.DataFrame({
        'model_type': ['full_model', 'top5_features_model'],
        'train_accuracy': [train_score, train_score_top5],
        'test_accuracy': [test_score, test_score_top5],
        'features_count': [X_train.shape[1], 5],
        'run_timestamp': [pd.Timestamp.now(), pd.Timestamp.now()]
    })
    
    # Save feature importance to database
    feature_importance_df['run_timestamp'] = pd.Timestamp.now()
    
    # Save results to PostgreSQL
    with engine.connect() as connection:
        # Create tables if they don't exist
        connection.execute("""
        CREATE SCHEMA IF NOT EXISTS ml_results;
        
        CREATE TABLE IF NOT EXISTS ml_results.iris_model_metrics (
            id SERIAL PRIMARY KEY,
            model_type VARCHAR(100),
            train_accuracy FLOAT,
            test_accuracy FLOAT,
            features_count INTEGER,
            run_timestamp TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS ml_results.iris_feature_importance (
            id SERIAL PRIMARY KEY,
            feature VARCHAR(100),
            importance FLOAT,
            run_timestamp TIMESTAMP
        );
        """)
        
        # Save model metrics
        results_df.to_sql('iris_model_metrics', connection, schema='ml_results', 
                          if_exists='append', index=False)
        
        # Save feature importance
        feature_importance_df.to_sql('iris_feature_importance', connection, schema='ml_results', 
                                    if_exists='append', index=False)
    
    # Return the top features and model metrics for XCom
    return {
        'top_features': top_features,
        'full_model_accuracy': test_score,
        'top5_model_accuracy': test_score_top5
    }

if __name__ == "__main__":
    process_iris_data()