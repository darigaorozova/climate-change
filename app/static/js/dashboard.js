document.addEventListener('DOMContentLoaded', function() {
    updateDashboard(); 
});

async function updateDashboard() {
    const startYear = document.getElementById('start-year').value;
    const endYear = document.getElementById('end-year').value;
    const groupBy = document.getElementById('group-by').value;

    // Валидация
    if (parseInt(startYear) > parseInt(endYear)) {
        alert("Ошибка: Год начала не может быть больше года окончания!");
        return;
    }

    const url = `/api/dashboard-drilldown?start_year=${startYear}&end_year=${endYear}&group_by=${groupBy}`;
    
    try {
        const res = await fetch(url);
        const data = await res.json();

        // Настройка заголовков осей в зависимости от группировки
        const xTitle = groupBy === 'year' ? 'Год' : 'Год-Месяц';
        
        // --- ГРАФИК 1: ТЕМПЕРАТУРА ---
        const traceTemp = {
            x: data.labels,
            y: data.temperatures,
            type: 'scatter',
            mode: 'lines+markers', // Линии с точками
            name: 'Средняя t°C',
            line: {color: '#FF6B6B', width: 2},
            marker: {size: 6}
        };

        const layoutTemp = {
            title: `Средняя температура (${groupBy === 'year' ? 'По годам' : 'Детализация по месяцам'})`,
            xaxis: {
                title: xTitle,
                // Если смотрим по месяцам, добавляем ползунок (Range Slider), так как точек много
                rangeslider: {visible: groupBy === 'month'} 
            },
            yaxis: {title: 'Температура (°C)'},
            template: 'plotly_white',
            margin: {t: 50, l: 50, r: 20, b: 50},
            height: 500
        };

        Plotly.newPlot('chart-temp', [traceTemp], layoutTemp, {responsive: true});

        // --- ГРАФИК 2: ОСАДКИ ---
        const tracePrecip = {
            x: data.labels,
            y: data.precipitation,
            type: 'bar', // Столбчатая диаграмма для осадков лучше
            name: 'Сумма осадков',
            marker: {
                color: '#4ECDC4',
                opacity: 0.8
            }
        };

        const layoutPrecip = {
            title: `Сумма осадков (${groupBy === 'year' ? 'По годам' : 'Детализация по месяцам'})`,
            xaxis: {
                title: xTitle,
                rangeslider: {visible: groupBy === 'month'}
            },
            yaxis: {title: 'Осадки (мм)'},
            template: 'plotly_white',
            margin: {t: 50, l: 50, r: 20, b: 50},
            height: 500
        };

        Plotly.newPlot('chart-precip', [tracePrecip], layoutPrecip, {responsive: true});

    } catch (e) {
        console.error("Ошибка загрузки:", e);
        alert("Не удалось загрузить данные. Проверьте консоль.");
    }
}