const fs = require('fs').promises;
const path = require('path');

// Функция для парсинга координат (простая версия для строгого формата)
function parseLatLng(latLngString) {
    if (!latLngString || typeof latLngString !== 'string') {
        return { latitude: '', longitude: '' };
    }
    
    // Строгий формат: "55.6843886°, 37.5837741°"
    // Просто удаляем символ градуса и разделяем по запятой
    const parts = latLngString.replace(/°/g, '').split(',').map(s => s.trim());
    
    if (parts.length >= 2) {
        return {
            latitude: parts[0],
            longitude: parts[1]
        };
    }
    
    return { latitude: '', longitude: '' };
}

// Асинхронная обработка файла
async function processJsonFileAsync(filePath) {
    try {
        console.log(`Чтение файла: ${filePath}`);
        
        // Читаем JSON-файл асинхронно
        const rawData = await fs.readFile(filePath, 'utf8');
        const data = JSON.parse(rawData);
        
        console.log(`Файл прочитан, элементов: ${data.semanticSegments?.length || 0}`);
        
        const rows = [];
        let processedCount = 0;
        
        // Обрабатываем каждый segment
        for (const segment of data.semanticSegments || []) {
            processedCount++;
            
            // Случай 1: activity
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
            
            // Случай 2: visit
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
            
            // Случай 3: timelinePath
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
            
            // Логирование прогресса для больших файлов
            if (processedCount % 1000 === 0) {
                console.log(`Обработано ${processedCount} сегментов, найдено ${rows.length} записей...`);
            }
        }
        
        console.log(`Обработка завершена. Всего сегментов: ${processedCount}, записей: ${rows.length}`);
        return rows;
        
    } catch (error) {
        console.error('Ошибка при обработке файла:', error.message);
        throw error;
    }
}

// Асинхронное сохранение в CSV
async function saveToCSVAsync(rows, outputPath) {
    try {
        console.log(`Сохранение в CSV: ${outputPath}`);
        
        // Заголовки CSV
        const headers = ['startTime', 'endTime', 'probability', 'latitude', 'longitude', 'source'];
        
        // Создаем поток для записи (эффективно для больших файлов)
        const writeStream = require('fs').createWriteStream(outputPath, { encoding: 'utf8' });
        
        // Пишем заголовки
        writeStream.write(headers.join(',') + '\n');
        
        // Пишем данные построчно
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const csvRow = [
                `"${escapeCSV(row.startTime)}"`,
                `"${escapeCSV(row.endTime)}"`,
                row.probability !== '' ? row.probability : '""',
                `"${escapeCSV(row.latitude)}"`,
                `"${escapeCSV(row.longitude)}"`,
                `"${escapeCSV(row.source)}"`
            ].join(',') + '\n';
            
            writeStream.write(csvRow);
            
            // Логирование прогресса для больших файлов
            if (i > 0 && i % 10000 === 0) {
                console.log(`Записано ${i} строк из ${rows.length}...`);
            }
        }
        
        // Закрываем поток и ждем завершения
        await new Promise((resolve, reject) => {
            writeStream.end();
            writeStream.on('finish', resolve);
            writeStream.on('error', reject);
        });
        
        console.log(`CSV файл сохранен: ${outputPath} (${rows.length} строк)`);
        
    } catch (error) {
        console.error('Ошибка при сохранении CSV:', error.message);
        throw error;
    }
}

// Экранирование для CSV
function escapeCSV(value) {
    if (value === null || value === undefined) return '';
    return String(value).replace(/"/g, '""');
}

// Асинхронное сохранение в JSON
async function saveToJSONAsync(rows, outputPath) {
    try {
        console.log(`Сохранение в JSON: ${outputPath}`);
        
        // Для больших файлов лучше писать потоком
        const writeStream = require('fs').createWriteStream(outputPath, { encoding: 'utf8' });
        
        // Начало массива
        writeStream.write('[\n');
        
        // Пишем каждую строку
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const jsonRow = JSON.stringify(row);
            writeStream.write(jsonRow);
            
            if (i < rows.length - 1) {
                writeStream.write(',\n');
            } else {
                writeStream.write('\n');
            }
            
            // Логирование прогресса
            if (i > 0 && i % 10000 === 0) {
                console.log(`Записано ${i} объектов JSON из ${rows.length}...`);
            }
        }
        
        // Конец массива
        writeStream.write(']');
        
        // Закрываем поток
        await new Promise((resolve, reject) => {
            writeStream.end();
            writeStream.on('finish', resolve);
            writeStream.on('error', reject);
        });
        
        console.log(`JSON файл сохранен: ${outputPath}`);
        
    } catch (error) {
        console.error('Ошибка при сохранении JSON:', error.message);
        throw error;
    }
}

// Потоковая обработка для ОЧЕНЬ больших файлов
async function processJsonFileStreaming(filePath, batchSize = 1000) {
    return new Promise((resolve, reject) => {
        const rows = [];
        let buffer = '';
        let inArray = false;
        let objectDepth = 0;
        let segmentCount = 0;
        
        const readStream = require('fs').createReadStream(filePath, { encoding: 'utf8', highWaterMark: 64 * 1024 }); // 64KB chunks
        
        readStream.on('data', (chunk) => {
            buffer += chunk;
            
            // Обрабатываем объекты из буфера
            let startPos = 0;
            
            if (!inArray) {
                // Ищем начало массива semanticSegments
                const arrayStart = buffer.indexOf('"semanticSegments":[');
                if (arrayStart !== -1) {
                    startPos = arrayStart + 20; // после "["
                    inArray = true;
                    buffer = buffer.slice(startPos);
                }
                return;
            }
            
            // Парсим объекты из массива
            while (buffer.length > 0) {
                // Ищем начало объекта
                if (objectDepth === 0) {
                    const objStart = buffer.indexOf('{');
                    if (objStart === -1) {
                        buffer = '';
                        break;
                    }
                    buffer = buffer.slice(objStart);
                }
                
                // Парсим объект
                let i = 0;
                let inString = false;
                let escapeNext = false;
                
                for (; i < buffer.length; i++) {
                    const char = buffer[i];
                    
                    if (escapeNext) {
                        escapeNext = false;
                        continue;
                    }
                    
                    if (char === '\\') {
                        escapeNext = true;
                        continue;
                    }
                    
                    if (char === '"') {
                        inString = !inString;
                        continue;
                    }
                    
                    if (!inString) {
                        if (char === '{') {
                            objectDepth++;
                        } else if (char === '}') {
                            objectDepth--;
                            if (objectDepth === 0) {
                                // Найден полный объект
                                const jsonStr = buffer.slice(0, i + 1);
                                try {
                                    const segment = JSON.parse(jsonStr);
                                    processSegment(segment);
                                    segmentCount++;
                                    
                                    if (segmentCount % batchSize === 0) {
                                        console.log(`Обработано ${segmentCount} сегментов...`);
                                    }
                                } catch (e) {
                                    // Игнорируем ошибки парсинга
                                }
                                
                                buffer = buffer.slice(i + 1);
                                break;
                            }
                        }
                    }
                }
                
                if (i === buffer.length) {
                    // Не нашли полный объект
                    break;
                }
            }
        });
        
        readStream.on('end', () => {
            console.log(`Потоковая обработка завершена. Сегментов: ${segmentCount}, записей: ${rows.length}`);
            resolve(rows);
        });
        
        readStream.on('error', reject);
        
        function processSegment(segment) {
            // Та же логика обработки, что и раньше
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
    });
}

// Вывод статистики
function printStatistics(rows) {
    console.log('\n=== СТАТИСТИКА ===');
    console.log(`Всего записей: ${rows.length}`);
    
    if (rows.length === 0) return;
    
    // Подсчет по источникам
    const sourceStats = {};
    rows.forEach(row => {
        sourceStats[row.source] = (sourceStats[row.source] || 0) + 1;
    });
    
    console.log('\nРаспределение по источникам:');
    for (const [source, count] of Object.entries(sourceStats)) {
        const percentage = ((count / rows.length) * 100).toFixed(1);
        console.log(`  ${source}: ${count} (${percentage}%)`);
    }
    
    // Проверка координат
    const invalidCoords = rows.filter(row => !row.latitude || !row.longitude);
    if (invalidCoords.length > 0) {
        console.log(`\n⚠️  Записей с некорректными координатами: ${invalidCoords.length}`);
    }
    
    // Примеры данных
    console.log('\nПримеры записей (первые 3):');
    rows.slice(0, 3).forEach((row, i) => {
        console.log(`  ${i + 1}. ${row.startTime} | ${row.latitude}, ${row.longitude} | ${row.source}`);
    });
}

// Основная асинхронная функция
async function mainAsync(useStreaming = false) {
    try {
        const inputFile = 'Хронология.json';
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
        
        console.log(`Начало обработки в ${new Date().toLocaleTimeString()}`);
        
        let resultRows;
        
        // Выбор метода обработки
        if (useStreaming) {
            console.log('Используется потоковая обработка...');
            resultRows = await processJsonFileStreaming(inputFile);
        } else {
            console.log('Используется стандартная обработка...');
            resultRows = await processJsonFileAsync(inputFile);
        }
        
        if (resultRows.length === 0) {
            console.log('Нет данных для обработки.');
            return;
        }
        
        // Выводим статистику
        printStatistics(resultRows);
        
        // Сохраняем результаты
        const csvFile = `хронология_таблица_${timestamp}.csv`;
        const jsonFile = `хронология_таблица_${timestamp}.json`;
        
        // Сохраняем параллельно для скорости
        await Promise.all([
            saveToCSVAsync(resultRows, csvFile),
            saveToJSONAsync(resultRows, jsonFile)
        ]);
        
        console.log(`\nОбработка завершена в ${new Date().toLocaleTimeString()}`);
        console.log(`Результаты сохранены в:\n  - ${csvFile}\n  - ${jsonFile}`);
        
    } catch (error) {
        console.error('Ошибка в mainAsync:', error.message);
        process.exit(1);
    }
}

// Обработка аргументов командной строки
async function main() {
    const args = process.argv.slice(2);
    const useStreaming = args.includes('--stream') || args.includes('-s');
    const inputFile = args.find(arg => !arg.startsWith('--') && !arg.startsWith('-')) || 'Хронология.json';
    
    // Проверяем существование файла
    try {
        await fs.access(inputFile);
    } catch {
        console.error(`Файл "${inputFile}" не найден!`);
        console.log('Использование: node script.js [файл.json] [--stream]');
        process.exit(1);
    }
    
    console.log(`Обработка файла: ${inputFile}`);
    console.log(`Режим: ${useStreaming ? 'потоковый' : 'стандартный'}`);
    
    await mainAsync(useStreaming);
}

// Экспорт функций
module.exports = {
    parseLatLng,
    processJsonFileAsync,
    processJsonFileStreaming,
    saveToCSVAsync,
    saveToJSONAsync,
    printStatistics
};

// Запуск
if (require.main === module) {
    main().catch(error => {
        console.error('Необработанная ошибка:', error);
        process.exit(1);
    });
}