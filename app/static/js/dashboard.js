let currentData = null;

// Загрузка данных дашборда
async function loadDashboardData(startYear = null, endYear = null) {
    try {
        let url = '/api/dashboard-data';
        const params = new URLSearchParams();
        if (startYear) params.append('start_year', startYear);
        if (endYear) params.append('end_year', endYear);
        
        if (params.toString()) {
            url += '?' + params.toString();
        }
        
        const response = await fetch(url);
        currentData = await response.json();
        
        updateSummary(currentData.summary);
        updateCharts(currentData);
    } catch (error) {
        console.error('Error loading dashboard data:', error);
    }
}

// Обновление сводки
function updateSummary(summary) {
    document.getElementById('summary-temp').textContent = summary.avg_temp.toFixed(2) + ' °C';
    document.getElementById('summary-precip').textContent = summary.total_precip.toFixed(2) + ' мм';
    document.getElementById('summary-co2').textContent = summary.avg_co2.toFixed(2) + ' ppm';
}

// Обновление графиков
function updateCharts(data) {
    // График температуры
    const tempTrace = {
        x: data.years,
        y: data.temperatures,
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Температура',
        line: {color: '#FF6B6B', width: 3},
        marker: {size: 6}
    };
    
    const tempLayout = {
        title: 'Температура',
        xaxis: {title: 'Год'},
        yaxis: {title: 'Температура (°C)'},
        hovermode: 'x unified',
        template: 'plotly_white'
    };
    
    Plotly.newPlot('dashboard-temp-chart', [tempTrace], tempLayout, {responsive: true});
    
    // График осадков
    const precipTrace = {
        x: data.years,
        y: data.precipitation,
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Осадки',
        line: {color: '#4ECDC4', width: 3},
        fill: 'tozeroy',
        marker: {size: 6}
    };
    
    const precipLayout = {
        title: 'Осадки',
        xaxis: {title: 'Год'},
        yaxis: {title: 'Осадки (мм)'},
        hovermode: 'x unified',
        template: 'plotly_white'
    };
    
    Plotly.newPlot('dashboard-precip-chart', [precipTrace], precipLayout, {responsive: true});
    
    // График CO2
    const co2Trace = {
        x: data.years,
        y: data.co2_levels,
        type: 'scatter',
        mode: 'lines+markers',
        name: 'CO₂',
        line: {color: '#95E1D3', width: 3},
        marker: {size: 6}
    };
    
    const co2Layout = {
        title: 'Уровень CO₂',
        xaxis: {title: 'Год'},
        yaxis: {title: 'CO₂ (ppm)'},
        hovermode: 'x unified',
        template: 'plotly_white'
    };
    
    Plotly.newPlot('dashboard-co2-chart', [co2Trace], co2Layout, {responsive: true});
    
    // График сравнения
    const comparisonTrace1 = {
        x: data.years,
        y: data.temperatures,
        type: 'bar',
        name: 'Температура',
        marker: {color: '#FF6B6B'},
        yaxis: 'y'
    };
    
    const comparisonTrace2 = {
        x: data.years,
        y: data.precipitation,
        type: 'bar',
        name: 'Осадки',
        marker: {color: '#4ECDC4'},
        yaxis: 'y2'
    };
    
    const comparisonLayout = {
        title: 'Сравнение метрик',
        xaxis: {title: 'Год'},
        yaxis: {
            title: 'Температура (°C)',
            side: 'left'
        },
        yaxis2: {
            title: 'Осадки (мм)',
            overlaying: 'y',
            side: 'right'
        },
        template: 'plotly_white',
        barmode: 'group'
    };
    
    Plotly.newPlot('comparison-chart', [comparisonTrace1, comparisonTrace2], comparisonLayout, {responsive: true});
    
    // График трендов
    const trendsTrace1 = {
        x: data.years,
        y: data.temperatures,
        type: 'scatter',
        mode: 'lines',
        name: 'Температура',
        line: {color: '#FF6B6B'}
    };
    
    const trendsTrace2 = {
        x: data.years,
        y: data.co2_levels.map(val => val / 10), // Нормализация для визуализации
        type: 'scatter',
        mode: 'lines',
        name: 'CO₂ (нормализовано)',
        line: {color: '#95E1D3'}
    };
    
    const trendsLayout = {
        title: 'Тренды',
        xaxis: {title: 'Год'},
        yaxis: {title: 'Значения'},
        template: 'plotly_white'
    };
    
    Plotly.newPlot('trends-chart', [trendsTrace1, trendsTrace2], trendsLayout, {responsive: true});
    
    // Roll-up анализ по годам
    const rollupData = {};
    data.years.forEach((year, index) => {
        if (!rollupData[year]) {
            rollupData[year] = {
                temp: 0,
                precip: 0,
                co2: 0,
                count: 0
            };
        }
        rollupData[year].temp += data.temperatures[index] || 0;
        rollupData[year].precip += data.precipitation[index] || 0;
        rollupData[year].co2 += data.co2_levels[index] || 0;
        rollupData[year].count++;
    });
    
    const rollupYears = Object.keys(rollupData).sort();
    const rollupTemps = rollupYears.map(year => rollupData[year].temp / rollupData[year].count);
    
    const rollupTrace = {
        x: rollupYears,
        y: rollupTemps,
        type: 'bar',
        name: 'Средняя температура по годам',
        marker: {color: '#667eea'}
    };
    
    const rollupLayout = {
        title: 'Агрегация по годам (Roll-up)',
        xaxis: {title: 'Год'},
        yaxis: {title: 'Средняя температура (°C)'},
        template: 'plotly_white'
    };
    
    Plotly.newPlot('rollup-year-chart', [rollupTrace], rollupLayout, {responsive: true});
}

// Применение фильтров
function applyFilters() {
    const startYear = document.getElementById('start-year').value;
    const endYear = document.getElementById('end-year').value;
    
    if (startYear && endYear && parseInt(startYear) > parseInt(endYear)) {
        alert('Год начала должен быть меньше года окончания');
        return;
    }
    
    loadDashboardData(startYear || null, endYear || null);
}

// Сброс фильтров
function resetFilters() {
    document.getElementById('start-year').value = '2000';
    document.getElementById('end-year').value = '2024';
    loadDashboardData();
}

// Drill-down функциональность (пример)
document.addEventListener('DOMContentLoaded', function() {
    loadDashboardData();
    
    // Добавляем обработчик клика для drill-down (пример)
    const charts = ['dashboard-temp-chart', 'dashboard-precip-chart', 'dashboard-co2-chart'];
    charts.forEach(chartId => {
        const chartDiv = document.getElementById(chartId);
        if (chartDiv) {
            chartDiv.on('plotly_click', function(data) {
                const point = data.points[0];
                alert(`Детализация: ${point.x} год, значение: ${point.y.toFixed(2)}`);
                // Здесь можно добавить более детальную визуализацию
            });
        }
    });
});

