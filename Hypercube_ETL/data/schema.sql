create_transformed_table = '''
CREATE TABLE IF NOT EXISTS transformed_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    DeliveryStart TIMESTAMP,
    initialForecastSpnGeneration FLOAT,
    ExecutedVolume FLOAT,
    RollingMedian_initialForecastSpnGeneration FLOAT,
    RollingMedian_ExecutedVolume FLOAT
);
'''

create_daily_aggregate_table = '''
CREATE TABLE IF NOT EXISTS daily_aggregate (
    id INT AUTO_INCREMENT PRIMARY KEY,
    DeliveryStart DATE,
    total_initialForecastSpnGeneration FLOAT,
    total_ExecutedVolume FLOAT
);
'''

create_weekly_aggregate_table = '''
CREATE TABLE IF NOT EXISTS weekly_aggregate (
    id INT AUTO_INCREMENT PRIMARY KEY,
    DeliveryStart DATE,
    total_initialForecastSpnGeneration FLOAT,
    total_ExecutedVolume FLOAT
);
'''