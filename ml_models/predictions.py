import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import Pipeline
import os
import sys
import pickle
from config import PROCESSED_DATA_DIR

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)


def train_temperature_model():
    """Обучение модели для прогнозирования температуры"""
    try:
        df = pd.read_csv(os.path.join(PROCESSED_DATA_DIR, "climate_combined.csv"))
        df = df.dropna(subset=['Year', 'Avg_Temp'])
        
        X = df[['Year']].values
        y = df['Avg_Temp'].values
        
        # Полиномиальная регрессия
        model = Pipeline([
            ('poly', PolynomialFeatures(degree=2)),
            ('linear', LinearRegression())
        ])
        
        model.fit(X, y)
        
        # Сохраняем модель
        model_path = os.path.join(BASE_DIR, 'models', 'temperature_model.pkl')
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        
        print("✅ Temperature model trained")
        return model
    except Exception as e:
        print(f"❌ Error training temperature model: {e}")
        return None


def predict_temperature(years):
    """Прогнозирование температуры на будущие годы"""
    try:
        model_path = os.path.join(BASE_DIR, 'models', 'temperature_model.pkl')
        if not os.path.exists(model_path):
            train_temperature_model()
        
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        
        years_array = np.array(years).reshape(-1, 1)
        predictions = model.predict(years_array)
        
        return predictions.tolist()
    except Exception as e:
        print(f"❌ Error predicting temperature: {e}")
        return None


def generate_predictions():
    """Генерация прогнозов и сохранение в файл"""
    try:
        df = pd.read_csv(os.path.join(PROCESSED_DATA_DIR, "climate_combined.csv"))
        max_year = int(df['Year'].max())
        
        # Прогноз на 5 лет вперед
        future_years = list(range(max_year + 1, max_year + 6))
        
        temp_predictions = predict_temperature(future_years)
        
        predictions_df = pd.DataFrame({
            'Year': future_years,
            'Predicted_Temp': temp_predictions,
        })
        
        predictions_df.to_csv(os.path.join(PROCESSED_DATA_DIR, "predictions.csv"), index=False)
        print("✅ Predictions generated")
        return predictions_df
    except Exception as e:
        print(f"❌ Error generating predictions: {e}")
        return None

if __name__ == "__main__":
    train_temperature_model()
    generate_predictions()


