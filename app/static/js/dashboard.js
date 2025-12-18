document.addEventListener('DOMContentLoaded', function() {
    updateDashboard(); 
});

async function updateDashboard() {
    // 1. Сбор параметров
    const startYear = document.getElementById('start-year').value;
    const endYear = document.getElementById('end-year').value;
    const groupBy = document.getElementById('group-by').value;
    const aggFunc = document.getElementById('agg-func').value;
    
    const showTemp = document.getElementById('show-temp').checked;
    const showPrecip = document.getElementById('show-precip').checked;

    // 2. Валидация
    if (parseInt(startYear) > parseInt(endYear)) {
        alert("Ошибка: Год начала не может быть больше года окончания!");
        return;
    }

    // 3. Управление видимостью блоков
    document.getElementById('container-temp').style.display = showTemp ? 'block' : 'none';
    document.getElementById('container-precip').style.display = showPrecip ? 'block' : 'none';
    document.getElementById('spacer').style.display = (showTemp && showPrecip) ? 'block' : 'none';

    // 4. Запрос к API
    const url = `/api/dashboard-drilldown?start_year=${startYear}&end_year=${endYear}&group_by=${groupBy}&agg_func=${aggFunc}`;
    
    try {
        const res = await fetch(url);
        const data = await res.json();

        const xTitle = getXAxisTitle(groupBy);
        const aggLabel = aggFunc.toUpperCase(); // "AVG", "MAX"

        // --- ГРАФИК 1: ТЕМПЕРАТУРА ---
        if (showTemp) {
            const traceTemp = {
                x: data.labels,
                y: data.temperatures,
                type: 'scatter',
                mode: groupBy === 'day' ? 'lines' : 'lines+markers', // Для дней точек слишком много
                name: `${aggLabel} t°C`,
                line: {color: '#FF6B6B', width: 2}
            };

            const layoutTemp = {
                title: `${aggLabel} Температура (${xTitle})`,
                xaxis: {
                    title: xTitle,
                    rangeslider: {visible: groupBy !== 'year'} // Слайдер для месяцев и дней
                },
                yaxis: {title: 'Температура (°C)'},
                template: 'plotly_white',
                margin: {t: 50, l: 50, r: 20, b: 50}
            };

            Plotly.newPlot('chart-temp', [traceTemp], layoutTemp, {responsive: true});
        }

        // --- ГРАФИК 2: ОСАДКИ ---
        if (showPrecip) {
            const tracePrecip = {
                x: data.labels,
                y: data.precipitation,
                type: 'bar',
                name: 'Осадки',
                marker: {color: '#4ECDC4', opacity: 0.8}
            };

            const layoutPrecip = {
                title: `${aggLabel} Осадки (${xTitle})`, // Обычно осадки суммируют, но если пользователь выбрал MAX - покажем MAX
                xaxis: {
                    title: xTitle,
                    rangeslider: {visible: groupBy !== 'year'}
                },
                yaxis: {title: 'Осадки (мм)'},
                template: 'plotly_white',
                margin: {t: 50, l: 50, r: 20, b: 50}
            };

            Plotly.newPlot('chart-precip', [tracePrecip], layoutPrecip, {responsive: true});
        }

    } catch (e) {
        console.error("Ошибка:", e);
    }
}

function getXAxisTitle(groupBy) {
    if (groupBy === 'year') return 'Год';
    if (groupBy === 'month') return 'Год-Месяц';
    return 'Дата';
}