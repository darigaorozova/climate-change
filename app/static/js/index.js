document.addEventListener('DOMContentLoaded', function() {
    loadKPI();
    loadTrendChart();
    loadHistogramChart();
    loadDiagnosticChart();
    loadPredictiveChart();
    loadPrescriptiveAnalytics

});

async function loadKPI() {
    try {
        const res = await fetch('/api/kpi');
        const data = await res.json();
        
        // Обновляем год в заголовке
        document.getElementById('kpi-year-label').textContent = data.year;

        // --- 1. ТЕМПЕРАТУРНАЯ АНОМАЛИЯ ---
        const anomalyElem = document.getElementById('kpi-anomaly-val');
        const badgeElem = document.getElementById('kpi-anomaly-badge');
        
        // Ставим плюс, если число положительное
        const sign = data.temp_anomaly > 0 ? '+' : '';
        anomalyElem.textContent = `${sign}${data.temp_anomaly}`;
        
        // Логика цвета и текста
        if (data.temp_anomaly > 0) {
            badgeElem.textContent = "🔥 Теплее нормы";
            badgeElem.className = "kpi-badge delta-negative"; // Красный (плохо для климата)
            // Или можно создать стиль .bg-hot { background: #ffebee; color: #c62828; }
        } else if (data.temp_anomaly < 0) {
            badgeElem.textContent = "❄️ Холоднее нормы";
            badgeElem.className = "kpi-badge delta-positive"; // Зеленый/Синий
        } else {
            badgeElem.textContent = "✅ В пределах нормы";
            badgeElem.className = "kpi-badge delta-neutral";
        }

        // --- 2. ЭКСТРЕМАЛЬНЫЕ ДНИ ---
        document.getElementById('kpi-extreme-val').textContent = data.extreme_days;
        
        const extBadge = document.getElementById('kpi-extreme-badge');
        // Сравниваем с историческим средним
        const diff = data.extreme_days - data.extreme_hist_avg;
        
        if (diff > 0) {
            extBadge.textContent = `🔺 На ${Math.round(diff)} дней больше нормы`;
            extBadge.className = "kpi-badge delta-negative"; // Тревога
        } else {
            extBadge.textContent = "📉 Ниже или равно норме";
            extBadge.className = "kpi-badge delta-positive"; // Спокойно
        }

    } catch (error) {
        console.error("Ошибка загрузки KPI:", error);
    }
}

// --- ГРАФИК 1: ТРЕНД ---
async function loadTrendChart() {
    try {
        const response = await fetch('/api/descriptive/trend');
        const data = await response.json();

        // 1. Реальные данные (серые точки/линия)
        const traceRaw = {
            x: data.years,
            y: data.avg_temp,
            type: 'scatter',
            mode: 'markers+lines',
            name: 'Среднегодовая t°C',
            line: {color: '#cfd8dc', width: 1}, // Светло-серый
            marker: {size: 4, color: '#b0bec5'}
        };

        // 2. Тренд (жирная красная линия)
        const traceTrend = {
            x: data.years,
            y: data.trend,
            type: 'scatter',
            mode: 'lines',
            name: 'Климатический тренд (10 лет)',
            line: {color: '#FF5252', width: 4} // Ярко-красный
        };

        const layout = {
            title: 'Изменение климата в Кыргызстане (1940-202X)',
            xaxis: {title: 'Год'},
            yaxis: {title: 'Температура (°C)'},
            template: 'plotly_white',
            legend: {orientation: 'h', y: -0.1}, // Легенда снизу
            margin: {t: 50, l: 50, r: 20, b: 50}, // Отступы
            height: 500, // Явно задаем высоту для JS
            autosize: true
        };

        Plotly.newPlot('desc-trend-chart', [traceRaw, traceTrend], layout, {responsive: true});

    } catch (error) {
        console.error('Ошибка загрузки тренда:', error);
    }
}

// --- ГРАФИК 2: ГИСТОГРАММА ---
async function loadHistogramChart() {
    try {
        const response = await fetch('/api/descriptive/histogram');
        const data = await response.json();

        const trace = {
            x: data.bins,
            y: data.freq,
            type: 'bar',
            name: 'Часов наблюдения',
            // Градиентная раскраска: Синий (холод) -> Красный (жара)
            marker: {
                color: data.bins,
                colorscale: 'RdBu', 
                reversescale: true, // Чтобы синий был слева (минус), красный справа (плюс)
                colorbar: {title: 't°C'}
            }
        };

        const layout = {
            title: 'Частота температурных режимов',
            xaxis: {title: 'Температура (°C)'},
            yaxis: { type: 'log', title: 'Частота (лог. шкала)' },
            template: 'plotly_white',
            margin: {t: 50, l: 60, r: 20, b: 60},
            height: 500, // Явно задаем высоту для JS
            autosize: true
        };

        Plotly.newPlot('desc-histogram', [trace], layout, {responsive: true});

    } catch (error) {
        console.error('Ошибка загрузки гистограммы:', error);
    }
}

// --- ДИАГНОСТИКА (Корреляции) ---
async function loadDiagnosticChart() {
    try {
        const res = await fetch('/api/diagnostic/correlations');
        const data = await res.json();

        const trace = {
            x: data.values,       
            y: data.names,        
            type: 'bar',
            orientation: 'h',     
            marker: {
                color: data.colors, 
                width: 0.6,
                line: { width: 1, color: '#333' } // Добавим обводку для красоты
            },
            text: data.values,    
            textposition: 'auto',
            hoverinfo: 'x+y'
        };

        const layout = {
            title: 'Матрица влияния факторов (Корреляция)',
            xaxis: {
                title: 'Сила влияния (от -1 до +1)', 
                range: [-1.1, 1.1], // Чуть шире, чтобы текст влез
                zeroline: true,
                zerolinewidth: 2,
                zerolinecolor: '#444'
            },
            yaxis: {
                automargin: true,
                tickfont: {size: 14}
            },
            template: 'plotly_white',
            margin: {l: 150, r: 20, t: 40, b: 40},
            height: 450,
            autosize: true
        };

        Plotly.newPlot('diagnostic-corr-chart', [trace], layout, {responsive: true});

    } catch (error) {
        console.error("Ошибка загрузки диагностики:", error);
    }
}

async function loadPredictiveChart() {
    try {
        const res = await fetch('/api/predictive-temp');
        const data = await res.json();
        
        if(data.data && data.data.length > 0) {
            // Добавляем настройки адаптивности и высоты
            const layout = {
                ...data.layout,
                height: 450,
                margin: {l: 50, r: 20, t: 50, b: 50},
                legend: {orientation: 'h', y: -0.2}
            };
            
            const config = {responsive: true};
            
            Plotly.newPlot('predictive-chart', data.data, layout, config);
        } else {
            document.getElementById('predictive-chart').innerHTML = 
                '<p style="text-align:center; padding: 20px;">Модель еще не готова или нет данных.</p>';
        }
    } catch (e) {
        console.error("Ошибка загрузки прогноза:", e);
    }
}

async function loadPrescriptiveAnalytics() {
    try {
        const res = await fetch('/api/prescriptive');
        const data = await res.json();
        
        // 1. Обновляем бейдж с общим прогнозом
        if (data.forecast_summary) {
            document.getElementById('forecast-summary-badge').textContent = data.forecast_summary;
        }

        // 2. Генерируем карточки
        const container = document.getElementById('prescriptive-container');
        container.innerHTML = ''; // Очистка

        if (data.recs && data.recs.length > 0) {
            data.recs.forEach(rec => {
                const card = document.createElement('div');
                card.className = `rec-card rec-status-${rec.status}`;
                
                card.innerHTML = `
                    <div class="rec-header">
                        <span>${rec.icon}</span>
                        <span>${rec.sector}</span>
                    </div>
                    <div class="rec-action">${rec.action}</div>
                    <div class="rec-detail">${rec.detail}</div>
                `;
                
                container.appendChild(card);
            });
        } else {
            container.innerHTML = '<p>Нет данных для рекомендаций.</p>';
        }

    } catch (error) {
        console.error("Ошибка рекомендаций:", error);
    }
}
