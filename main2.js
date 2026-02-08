const fs = require('fs').promises;
const XLSX = require('xlsx');

function parseLatLng(latLngString) {
    if (!latLngString) return { lat: '', lng: '' };
    const parts = latLngString.replace(/°/g, '').split(',').map(s => s.trim());
    return parts.length >= 2 
        ? { lat: parseFloat(parts[0]), lng: parseFloat(parts[1]) }
        : { lat: '', lng: '' };
}

async function processToExcel(inputFile, outputFile) {
    try {
        // Чтение и обработка данных
        const jsonData = JSON.parse(await fs.readFile(inputFile, 'utf8'));
        const workbook = XLSX.utils.book_new();
        
        // Массивы для разных типов данных
        const activities = [];
        const visits = [];
        const timelinePaths = [];
        
        // Обработка каждого сегмента
        jsonData.semanticSegments?.forEach(segment => {
            if (segment.activity) {
                const { activity } = segment;
                
                if (activity.start?.latLng) {
                    const { lat, lng } = parseLatLng(activity.start.latLng);
                    activities.push({
                        'Start Time': segment.startTime,
                        'End Time': segment.endTime,
                        'Probability': activity.topCandidate?.probability || 0,
                        'Latitude': lat,
                        'Longitude': lng,
                        'Type': 'Start',
                        'Activity Type': activity.topCandidate?.type || 'UNKNOWN'
                    });
                }
                
                if (activity.end?.latLng) {
                    const { lat, lng } = parseLatLng(activity.end.latLng);
                    activities.push({
                        'Start Time': segment.startTime,
                        'End Time': segment.endTime,
                        'Probability': activity.topCandidate?.probability || 0,
                        'Latitude': lat,
                        'Longitude': lng,
                        'Type': 'End',
                        'Activity Type': activity.topCandidate?.type || 'UNKNOWN'
                    });
                }
            }
            else if (segment.visit) {
                const { visit } = segment;
                
                if (visit.topCandidate?.placeLocation?.latLng) {
                    const { lat, lng } = parseLatLng(visit.topCandidate.placeLocation.latLng);
                    visits.push({
                        'Start Time': segment.startTime,
                        'End Time': segment.endTime,
                        'Probability': visit.probability || 0,
                        'Latitude': lat,
                        'Longitude': lng,
                        'Place ID': visit.topCandidate.placeId || '',
                        'Place Type': visit.topCandidate.semanticType || '',
                        'Hierarchy': visit.hierarchyLevel || 0
                    });
                }
            }
            else if (segment.timelinePath) {
                segment.timelinePath.forEach(point => {
                    if (point.point && point.time) {
                        const { lat, lng } = parseLatLng(point.point);
                        timelinePaths.push({
                            'Time': point.time,
                            'Latitude': lat,
                            'Longitude': lng,
                            'Segment Start': segment.startTime,
                            'Segment End': segment.endTime
                        });
                    }
                });
            }
        });
        
        // Создаем листы
        if (activities.length > 0) {
            const ws1 = XLSX.utils.json_to_sheet(activities);
            XLSX.utils.book_append_sheet(workbook, ws1, 'Activities');
        }
        
        if (visits.length > 0) {
            const ws2 = XLSX.utils.json_to_sheet(visits);
            XLSX.utils.book_append_sheet(workbook, ws2, 'Visits');
        }
        
        if (timelinePaths.length > 0) {
            const ws3 = XLSX.utils.json_to_sheet(timelinePaths);
            XLSX.utils.book_append_sheet(workbook, ws3, 'Timeline Path');
        }
        
        // Лист со всеми данными
        const allData = [...activities.map(a => ({ ...a, 'Data Type': 'Activity' })), 
                        ...visits.map(v => ({ ...v, 'Data Type': 'Visit' })), 
                        ...timelinePaths.map(t => ({ ...t, 'Data Type': 'Timeline' }))];
        
        if (allData.length > 0) {
            const wsAll = XLSX.utils.json_to_sheet(allData);
            XLSX.utils.book_append_sheet(workbook, wsAll, 'All Data');
        }
        
        // Сохраняем Excel файл
        XLSX.writeFile(workbook, outputFile);
        
        console.log(`✅ Excel файл создан: ${outputFile}`);
        console.log(`📊 Activities: ${activities.length} записей`);
        console.log(`📍 Visits: ${visits.length} записей`);
        console.log(`🛣️  Timeline Points: ${timelinePaths.length} записей`);
        
        return {
            activities: activities.length,
            visits: visits.length,
            timelinePaths: timelinePaths.length,
            total: allData.length,
            file: outputFile
        };
        
    } catch (error) {
        console.error('❌ Ошибка:', error.message);
        throw error;
    }
}

// CLI интерфейс
async function main() {
    const args = process.argv.slice(2);
    const inputFile = args[0] || 'Хронология.json';
    const outputFile = args[1] || `хронология_отчет_${new Date().toISOString().slice(0, 10)}.xlsx`;
    
    console.log(`
    ================================
        Хронология в Excel
    ================================
    Входной файл:  ${inputFile}
    Выходной файл: ${outputFile}
    ================================
    `);
    
    try {
        await fs.access(inputFile);
        const result = await processToExcel(inputFile, outputFile);
        
        console.log(`
    ================================
           ОБРАБОТКА ЗАВЕРШЕНА
    ================================
    Всего записей: ${result.total}
    Файл: ${result.file}
    ================================
        `);
        
    } catch (error) {
        console.error(`
    ================================
             ОШИБКА
    ================================
    ${error.message}
    ================================
        `);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}