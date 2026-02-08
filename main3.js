const fs = require('fs').promises;
const XLSX = require('xlsx');

// Парсинг координат
function parseLatLng(latLngString) {
    if (!latLngString) return { latitude: '', longitude: '' };
    const parts = latLngString.replace(/°/g, '').split(',').map(s => s.trim());
    return parts.length >= 2 
        ? { latitude: parts[0], longitude: parts[1] }
        : { latitude: '', longitude: '' };
}

// Обработка JSON
async function processJsonFile(filePath) {
    const data = JSON.parse(await fs.readFile(filePath, 'utf8'));
    return (data.semanticSegments || []).flatMap(segment => {
        const results = [];
        
        if (segment.activity) {
            const { activity } = segment;
            if (activity.start?.latLng) {
                const { latitude, longitude } = parseLatLng(activity.start.latLng);
                results.push({ ...segment, ...activity, latitude, longitude, type: 'activity.start' });
            }
            if (activity.end?.latLng) {
                const { latitude, longitude } = parseLatLng(activity.end.latLng);
                results.push({ ...segment, ...activity, latitude, longitude, type: 'activity.end' });
            }
        } else if (segment.visit) {
            const { visit } = segment;
            if (visit.topCandidate?.placeLocation?.latLng) {
                const { latitude, longitude } = parseLatLng(visit.topCandidate.placeLocation.latLng);
                results.push({ ...segment, ...visit, latitude, longitude, type: 'visit' });
            }
        } else if (segment.timelinePath) {
            segment.timelinePath.forEach(point => {
                if (point.point && point.time) {
                    const { latitude, longitude } = parseLatLng(point.point);
                    results.push({
                        startTime: point.time,
                        endTime: point.time,
                        probability: '',
                        latitude,
                        longitude,
                        type: 'timelinePath',
                        ...point
                    });
                }
            });
        }
        
        return results;
    });
}

// Создание Excel с улучшенным форматированием
async function createExcelWithFormatting(data, outputPath) {
    const wb = XLSX.utils.book_new();
    
    // Подготовка данных для Excel
    const excelData = data.map(item => ({
        'Начало': item.startTime || '',
        'Конец': item.endTime || '',
        'Вероятность': item.probability || item.topCandidate?.probability || '',
        'Широта': item.latitude,
        'Долгота': item.longitude,
        'Тип': item.type,
        'Источник': item.type.includes('activity') ? 'Активность' : 
                   item.type.includes('visit') ? 'Посещение' : 'Путь',
        'ID места': item.topCandidate?.placeId || '',
        'Тип места': item.topCandidate?.semanticType || ''
    }));
    
    // Создаем лист с данными
    const ws = XLSX.utils.json_to_sheet(excelData);
    
    // Настраиваем ширину колонок
    const colWidths = [
        { wch: 25 }, // Начало
        { wch: 25 }, // Конец
        { wch: 12 }, // Вероятность
        { wch: 12 }, // Широта
        { wch: 12 }, // Долгота
        { wch: 15 }, // Тип
        { wch: 15 }, // Источник
        { wch: 30 }, // ID места
        { wch: 20 }  // Тип места
    ];
    ws['!cols'] = colWidths;
    
    // Добавляем заголовок
    const title = [['Хронология событий'], [''], ['']];
    XLSX.utils.sheet_add_aoa(ws, title, { origin: 'A1' });
    
    // Сдвигаем данные
    const range = XLSX.utils.decode_range(ws['!ref']);
    range.s.r = 2; // Сдвигаем на 2 строки вниз
    ws['!ref'] = XLSX.utils.encode_range(range);
    
    // Добавляем лист в книгу
    XLSX.utils.book_append_sheet(wb, ws, 'Данные');
    
    // Добавляем лист со сводкой
    const summaryData = [
        ['Сводка', ''],
        ['Всего записей', data.length],
        [''],
        ['Типы записей', 'Количество'],
        ...Object.entries(
            data.reduce((acc, item) => {
                const type = item.type;
                acc[type] = (acc[type] || 0) + 1;
                return acc;
            }, {})
        ).map(([type, count]) => [type, count])
    ];
    
    const summaryWs = XLSX.utils.aoa_to_sheet(summaryData);
    summaryWs['!cols'] = [{ wch: 25 }, { wch: 10 }];
    XLSX.utils.book_append_sheet(wb, summaryWs, 'Сводка');
    
    // Сохраняем файл
    XLSX.writeFile(wb, outputPath);
    console.log(`Excel файл создан: ${outputPath}`);
}

// Быстрая обработка
async function quickProcess() {
    const input = process.argv[2] || 'Хронология.json';
    const output = process.argv[3] || `хронология_${Date.now()}.xlsx`;
    
    try {
        console.log(`Обработка ${input}...`);
        const data = await processJsonFile(input);
        console.log(`Найдено ${data.length} записей`);
        
        await createExcelWithFormatting(data, output);
        console.log(`Готово! Файл сохранен как ${output}`);
        
    } catch (error) {
        console.error('Ошибка:', error.message);
    }
}

if (require.main === module) {
    quickProcess();
}