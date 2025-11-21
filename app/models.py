"""
Модуль для работы с ML моделями для предсказания климатических данных
"""
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import pickle
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODELS_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODELS_DIR, exist_ok=True)

class ClimatePredictor:
    """Класс для предсказания климатических данных"""
    
    def __init__(self):
        self.temp_model = None
        self.co2_model = None
        self.precip_model = None
        self.scaler = StandardScaler()
        
    def train_temperature_model(self, df):
        """Обучение модели для предсказания температуры"""
        # Подготовка данных
        df['year_numeric'] = df['Year']
        df['temp_lag1'] = df['avg_temp'].shift(1)
        df['temp_lag2'] = df['avg_temp'].shift(2)
        
        df_clean = df.dropna()
        
        X = df_clean[['year_numeric', 'co2_level', 'temp_lag1', 'temp_lag2']].values
        y = df_clean['avg_temp'].values
        
        X_scaled = self.scaler.fit_transform(X)
        
        # Используем Random Forest для лучшей точности
        self.temp_model = RandomForestRegressor(n_estimators=100, random_state=42)
        self.temp_model.fit(X_scaled, y)
        
        # Сохраняем модель
        model_path = os.path.join(MODELS_DIR, 'temperature_model.pkl')
        with open(model_path, 'wb') as f:
            pickle.dump(self.temp_model, f)
        
        print("✅ Temperature model trained and saved")
        return self.temp_model
    
    def train_co2_model(self, df):
        """Обучение модели для предсказания CO2"""
        df['year_numeric'] = df['Year']
        df['co2_lag1'] = df['co2_level'].shift(1)
        df['co2_lag2'] = df['co2_level'].shift(2)
        
        df_clean = df.dropna()
        
        X = df_clean[['year_numeric', 'co2_lag1', 'co2_lag2']].values
        y = df_clean['co2_level'].values
        
        self.co2_model = LinearRegression()
        self.co2_model.fit(X, y)
        
        model_path = os.path.join(MODELS_DIR, 'co2_model.pkl')
        with open(model_path, 'wb') as f:
            pickle.dump(self.co2_model, f)
        
        print("✅ CO2 model trained and saved")
        return self.co2_model
    
    def predict_temperature(self, df, years_ahead=5):
        """Предсказание температуры на несколько лет вперед"""
        if self.temp_model is None:
            # Попытка загрузить модель
            model_path = os.path.join(MODELS_DIR, 'temperature_model.pkl')
            if os.path.exists(model_path):
                with open(model_path, 'rb') as f:
                    self.temp_model = pickle.load(f)
            else:
                return None
        
        predictions = []
        last_data = df.tail(1).iloc[0]
        
        for i in range(1, years_ahead + 1):
            year = df['Year'].max() + i
            co2_estimate = last_data['co2_level'] * (1.01 ** i)  # Простая оценка роста CO2
            
            # Используем последние доступные значения
            temp_lag1 = last_data['avg_temp'] if i == 1 else predictions[-1]
            temp_lag2 = last_data['avg_temp'] if i <= 2 else predictions[-2]
            
            X = np.array([[year, co2_estimate, temp_lag1, temp_lag2]])
            X_scaled = self.scaler.transform(X)
            
            pred = self.temp_model.predict(X_scaled)[0]
            predictions.append(pred)
        
        return predictions
    
    def predict_co2(self, df, years_ahead=5):
        """Предсказание CO2 на несколько лет вперед"""
        if self.co2_model is None:
            model_path = os.path.join(MODELS_DIR, 'co2_model.pkl')
            if os.path.exists(model_path):
                with open(model_path, 'rb') as f:
                    self.co2_model = pickle.load(f)
            else:
                return None
        
        predictions = []
        last_data = df.tail(1).iloc[0]
        
        for i in range(1, years_ahead + 1):
            year = df['Year'].max() + i
            co2_lag1 = last_data['co2_level'] if i == 1 else predictions[-1]
            co2_lag2 = last_data['co2_level'] if i <= 2 else predictions[-2]
            
            X = np.array([[year, co2_lag1, co2_lag2]])
            pred = self.co2_model.predict(X)[0]
            predictions.append(pred)
        
        return predictions

