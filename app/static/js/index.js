// Загрузка KPI данных
async function loadKPI() {
    try {
        const response = await fetch('/api/kpi');
        const data = await response.json();
        
        document.getElementById('kpi-temp').textContent = data.avg_temp || '-';
        document.getElementById('kpi-precip').textContent = data.total_precipitation || '-';
        document.getElementById('kpi-events').textContent = data.extreme_events || '-';
        document.getElementById('kpi-co2').textContent = data.co2_level || '-';
    } catch (error) {
        console.error('Error loading KPI:', error);
    }
}

// Загрузка графика температуры
async function loadTemperatureChart() {
    try {
        const response = await fetch('/api/temperature-trend');
        const data = await response.json();
        Plotly.newPlot('temperature-chart', data.data, data.layout, {responsive: true});
    } catch (error) {
        console.error('Error loading temperature chart:', error);
        document.getElementById('temperature-chart').innerHTML = '<p>Ошибка загрузки данных</p>';
    }
}

// Загрузка графика осадков
async function loadPrecipitationChart() {
    try {
        const response = await fetch('/api/precipitation-trend');
        const data = await response.json();
        Plotly.newPlot('precipitation-chart', data.data, data.layout, {responsive: true});
    } catch (error) {
        console.error('Error loading precipitation chart:', error);
        document.getElementById('precipitation-chart').innerHTML = '<p>Ошибка загрузки данных</p>';
    }
}

// Загрузка графика экстремальных событий
async function loadExtremeEventsChart() {
    try {
        const response = await fetch('/api/extreme-events-bar');
        const data = await response.json();
        Plotly.newPlot('extreme-events-chart', data.data, data.layout, {responsive: true});
    } catch (error) {
        console.error('Error loading extreme events chart:', error);
        document.getElementById('extreme-events-chart').innerHTML = '<p>Ошибка загрузки данных</p>';
    }
}

// Загрузка Box Plot
async function loadBoxPlot() {
    try {
        const response = await fetch('/api/temperature-boxplot');
        const data = await response.json();
        Plotly.newPlot('boxplot-chart', data.data, data.layout, {responsive: true});
    } catch (error) {
        console.error('Error loading box plot:', error);
        document.getElementById('boxplot-chart').innerHTML = '<p>Ошибка загрузки данных</p>';
    }
}

// Загрузка картограммы температуры
async function loadTemperatureMap() {
    try {
        const response = await fetch('/api/temperature-map');
        const data = await response.json();
        Plotly.newPlot('temperature-map-chart', data.data, data.layout, {responsive: true});
    } catch (error) {
        console.error('Error loading temperature map:', error);
        document.getElementById('temperature-map-chart').innerHTML = '<p>Ошибка загрузки данных</p>';
    }
}

// Загрузка прогнозного графика
async function loadPredictiveChart() {
    try {
        const response = await fetch('/api/predictive-temp');
        const data = await response.json();
        Plotly.newPlot('predictive-chart', data.data, data.layout, {responsive: true});
    } catch (error) {
        console.error('Error loading predictive chart:', error);
        document.getElementById('predictive-chart').innerHTML = '<p>Ошибка загрузки данных. Возможно, требуется обучение модели.</p>';
    }
}

// Загрузка графика корреляции
async function loadCorrelationChart() {
    try {
        const response = await fetch('/api/correlation-scatter');
        const data = await response.json();
        Plotly.newPlot('correlation-chart', data.data, data.layout, {responsive: true});
    } catch (error) {
        console.error('Error loading correlation chart:', error);
        document.getElementById('correlation-chart').innerHTML = '<p>Ошибка загрузки данных</p>';
    }
}

// Инициализация всех графиков при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    loadKPI();
    loadTemperatureChart();
    loadPrecipitationChart();
    loadExtremeEventsChart();
    loadBoxPlot();
    loadTemperatureMap();
    loadPredictiveChart();
    loadCorrelationChart();
    
    // Обновление KPI каждые 5 минут
    setInterval(loadKPI, 300000);
});

