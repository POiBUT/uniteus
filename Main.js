const fs = require('fs').promises;
const XLSX = require('xlsx');
const path = require('path');

// Функция для парсинга координат
function parseLatLng(latLngString) {
    if (!latLngString) return { latitude: '', longitude: '' };
    
    const parts = latLngString.replace(/°/g, '').split(',').map(s => s.trim());
    return parts.length >= 2 
        ? { latitude: parts[0], longitude: parts[1] }
        : { latitude: '', longitude: '' };
}

// Асинхронная обработка файла
async function processJsonFileAsync(filePath) {
    try {
        console.log(`Чтение JSON файла: ${filePath}`);
        
        const rawData = await fs.readFile(filePath, 'utf8');
        const data = JSON.parse(rawData);
        
        const rows = [];
        
        // Обработка данных
        for (const segment of data.semanticSegments || []) {
            // Обработка activity
            if (segment.activity) {
                const activity = segment.activity;
                
                if (activity.start?.latLng) {
                    const { latitude, longitude } = parseLatLng(activity.start.latLng);
                    rows.push({
                        startTime: segment.startTime || '',
                        endTime: segment.endTime || '',
                        probability: activity.topCandidate?.probability || 0.0,
                        latitude,
                        longitude,
                        source: 'activity.start'
                    });
                }
                
                if (activity.end?.latLng) {
                    const { latitude, longitude } = parseLatLng(activity.end.latLng);
                    rows.push({
                        startTime: segment.startTime || '',
                        endTime: segment.endTime || '',
                        probability: activity.topCandidate?.probability || 0.0,
                        latitude,
                        longitude,
                        source: 'activity.end'
                    });
                }
            }
            
            // Обработка visit
            else if (segment.visit) {
                const visit = segment.visit;
                
                if (visit.topCandidate?.placeLocation?.latLng) {
                    const { latitude, longitude } = parseLatLng(visit.topCandidate.placeLocation.latLng);
                    rows.push({
                        startTime: segment.startTime || '',
                        endTime: segment.endTime || '',
                        probability: visit.probability || 0.0,
                        latitude,
                        longitude,
                        source: 'visit.placeLocation'
                    });
                }
            }
            
            // Обработка timelinePath
            else if (segment.timelinePath) {
                for (const pointData of segment.timelinePath) {
                    if (pointData.point && pointData.time) {
                        const { latitude, longitude } = parseLatLng(pointData.point);
                        rows.push({
                            startTime: pointData.time,
                            endTime: pointData.time,
                            probability: '',
                            latitude,
                            longitude,
                            source: 'timelinePath'
                        });
                    }
                }
            }
        }
        
        console.log(`Обработано ${rows.length} записей`);
        return rows;
        
    } catch (error) {
        console.error('Ошибка при обработке файла:', error.message);
        throw error;
    }
}

// Сохранение в Excel (XLSX)
async function saveToExcel(rows, outputPath, options = {}) {
    try {
        console.log(`Создание Excel файла: ${outputPath}`);
        
        // Создаем новую рабочую книгу
        const wb = XLSX.utils.book_new();
        
        // Преобразуем данные в рабочий лист
        const ws = XLSX.utils.json_to_sheet(rows, {
            header: ['startTime', 'endTime', 'probability', 'latitude', 'longitude', 'source'],
            skipHeader: false
        });
        
        // Настраиваем ширину колонок
        const colWidths = [
            { wch: 30 }, // startTime
            { wch: 30 }, // endTime
            { wch: 15 }, // probability
            { wch: 15 }, // latitude
            { wch: 15 }, // longitude
            { wch: 20 }  // source
        ];
        ws['!cols'] = colWidths;
        
        // Добавляем заголовок
        if (options.title) {
            XLSX.utils.sheet_add_aoa(ws, [[options.title]], { origin: "A1" });
            XLSX.utils.sheet_add_aoa(ws, [[""]], { origin: "A2" }); // Пустая строка
            // Сдвигаем данные на 2 строки вниз
            const range = XLSX.utils.decode_range(ws['!ref']);
            range.s.r = 2;
            ws['!ref'] = XLSX.utils.encode_range(range);
        }
        
        // Добавляем лист в книгу
        const sheetName = options.sheetName || 'Хронология';
        XLSX.utils.book_append_sheet(wb, ws, sheetName);
        
        // Добавляем второй лист со статистикой
        if (options.includeStats) {
            const stats = generateStatistics(rows);
            const statsWs = XLSX.utils.json_to_sheet(stats);
            XLSX.utils.book_append_sheet(wb, statsWs, 'Статистика');
        }
        
        // Сохраняем файл
        XLSX.writeFile(wb, outputPath);
        
        console.log(`Excel файл сохранен: ${outputPath}`);
        
        // Возвращаем информацию о файле
        const fileStats = await fs.stat(outputPath);
        return {
            path: outputPath,
            size: fileStats.size,
            rows: rows.length,
            sheets: wb.SheetNames.length
        };
        
    } catch (error) {
        console.error('Ошибка при сохранении Excel:', error.message);
        throw error;
    }
}

// Генерация статистики
function generateStatistics(rows) {
    const stats = [];
    
    // Основная статистика
    stats.push({ "Параметр": "Всего записей", "Значение": rows.length });
    
    // Статистика по источникам
    const sourceCounts = {};
    rows.forEach(row => {
        sourceCounts[row.source] = (sourceCounts[row.source] || 0) + 1;
    });
    
    stats.push({ "Параметр": "--- По источникам ---", "Значение": "" });
    Object.entries(sourceCounts).forEach(([source, count]) => {
        const percentage = ((count / rows.length) * 100).toFixed(1);
        stats.push({ 
            "Параметр": source, 
            "Значение": `${count} (${percentage}%)` 
        });
    });
    
    // Статистика по координатам
    const validCoords = rows.filter(row => row.latitude && row.longitude).length;
    stats.push({ "Параметр": "--- Координаты ---", "Значение": "" });
    stats.push({ "Параметр": "С валидными координатами", "Значение": validCoords });
    stats.push({ "Параметр": "Без координат", "Значение": rows.length - validCoords });
    
    // Временной диапазон
    if (rows.length > 0) {
        const times = rows.map(row => new Date(row.startTime)).filter(d => !isNaN(d));
        if (times.length > 0) {
            const minTime = new Date(Math.min(...times)).toLocaleString();
            const maxTime = new Date(Math.max(...times)).toLocaleString();
            stats.push({ "Параметр": "--- Время ---", "Значение": "" });
            stats.push({ "Параметр": "Начало", "Значение": minTime });
            stats.push({ "Параметр": "Конец", "Значение": maxTime });
        }
    }
    
    // Дата генерации
    stats.push({ "Параметр": "--- Инфо ---", "Значение": "" });
    stats.push({ "Параметр": "Сгенерировано", "Значение": new Date().toLocaleString() });
    
    return stats;
}

// Сохранение в несколько форматов
async function saveToMultipleFormats(rows, baseName) {
    const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-');
    const results = {};
    
    // Excel
    const excelFile = `${baseName}_${timestamp}.xlsx`;
    const excelInfo = await saveToExcel(rows, excelFile, {
        title: 'Хронология событий',
        sheetName: 'Данные',
        includeStats: true
    });
    results.excel = excelInfo;
    
    // CSV (для совместимости)
    const csvFile = `${baseName}_${timestamp}.csv`;
    const csvContent = [
        'startTime,endTime,probability,latitude,longitude,source',
        ...rows.map(row => `"${row.startTime}","${row.endTime}",${row.probability || ''},"${row.latitude}","${row.longitude}","${row.source}"`)
    ].join('\n');
    await fs.writeFile(csvFile, csvContent, 'utf8');
    results.csv = csvFile;
    
    // JSON (для проверки)
    const jsonFile = `${baseName}_${timestamp}.json`;
    await fs.writeFile(jsonFile, JSON.stringify(rows, null, 2), 'utf8');
    results.json = jsonFile;
    
    return results;
}

// Основная функция
async function main() {
    try {
        const inputFile = process.argv[2] || 'Хронология.json';
        const outputBase = process.argv[3] || 'хронология';
        
        console.log(`=== Обработка файла: ${inputFile} ===\n`);
        
        // Проверяем существование файла
        try {
            await fs.access(inputFile);
        } catch {
            console.error(`❌ Файл "${inputFile}" не найден!`);
            console.log('Использование: node script.js [входной.json] [префикс_выходного]');
            process.exit(1);
        }
        
        // Обрабатываем файл
        const rows = await processJsonFileAsync(inputFile);
        
        if (rows.length === 0) {
            console.log('⚠️  Нет данных для обработки.');
            return;
        }
        
        console.log(`\n✅ Обработано ${rows.length} записей\n`);
        
        // Сохраняем в несколько форматов
        console.log('💾 Сохранение результатов...');
        const savedFiles = await saveToMultipleFormats(rows, outputBase);
        
        console.log('\n✅ Результаты сохранены:');
        console.log(`📊 Excel: ${savedFiles.excel.path} (${savedFiles.excel.rows} строк)`);
        console.log(`📄 CSV:   ${savedFiles.csv}`);
        console.log(`📁 JSON:  ${savedFiles.json}`);
        
        // Выводим предпросмотр
        console.log('\n👀 Предпросмотр первых 3 строк:');
        console.table(rows.slice(0, 3));
        
    } catch (error) {
        console.error('\n❌ Ошибка:', error.message);
        process.exit(1);
    }
}

// Экспорт функций
module.exports = {
    processJsonFileAsync,
    saveToExcel,
    parseLatLng,
    generateStatistics
};

// Запуск
if (require.main === module) {
    main();
}