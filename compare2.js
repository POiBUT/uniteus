const fs = require('fs').promises;
const path = require('path');

// Функция для парсинга CSV
async function parseCSV(filePath) {
    const content = await fs.readFile(filePath, 'utf8');
    const lines = content.trim().split('\n');
    
    // Парсим заголовки (первая строка)
    const headerLine = lines[0];
    const headers = headerLine.split(',').map(header => header.trim());
    
    const records = [];
    // Используем классический for для производительности
    for (let i = 1; i < lines.length; i++) {
        const line = lines[i];
        
        // Парсим строку с учетом кавычек
        const values = [];
        let currentValue = '';
        let insideQuotes = false;
        
        for (let j = 0; j < line.length; j++) {
            const char = line[j];
            
            if (char === '"') {
                insideQuotes = !insideQuotes;
                currentValue += char;
            } else if (char === ',' && !insideQuotes) {
                values.push(currentValue);
                currentValue = '';
            } else {
                currentValue += char;
            }
        }
        values.push(currentValue); // Добавляем последнее значение
        
        // Убираем кавычки из значений
        const cleanValues = new Array(values.length);
        for (let j = 0; j < values.length; j++) {
            const value = values[j];
            if (value.startsWith('"') && value.endsWith('"')) {
                cleanValues[j] = value.slice(1, -1);
            } else {
                cleanValues[j] = value;
            }
        }
        
        const record = {};
        for (let j = 0; j < headers.length; j++) {
            record[headers[j]] = cleanValues[j] || '';
        }
        
        records.push(record);
    }
    
    return records;
}

// Функция для округления координат до 3 знаков
function roundCoordinate(coord) {
    if (!coord) return null;
    return Math.round(parseFloat(coord) * 1000) / 1000;
}

// Функция для проверки совпадения времени с учетом 30 минут
function isTimeMatch(time1, time2) {
    if (!time1 || !time2) return false;
    const date1 = new Date(time1);
    const date2 = new Date(time2);
    const diffMinutes = Math.abs(date1 - date2) / (1000 * 60);
    return diffMinutes <= 30;
}

// Основная функция для поиска совпадений с использованием классических циклов
async function findMatchingRecords() {
    try {
        // Читаем оба файла
        console.log('Чтение файлов...');
        const records1 = await parseCSV('хронология1.csv');
        const records2 = await parseCSV('хронология2.csv');
        
        console.log(`Загружено записей: хронология1.csv - ${records1.length}, хронология2.csv - ${records2.length}`);
        
        const matches = [];
        const processedIndices = new Set();
        
        // Предварительно вычисляем округленные координаты для records2
        const records2Lat = new Array(records2.length);
        const records2Lon = new Array(records2.length);
        for (let i = 0; i < records2.length; i++) {
            records2Lat[i] = roundCoordinate(records2[i].latitude);
            records2Lon[i] = roundCoordinate(records2[i].longitude);
        }
        
        // Используем классический for для максимальной производительности
        for (let i = 0; i < records1.length; i++) {
            const record1 = records1[i];
            
            if (!record1.latitude || !record1.longitude) continue;
            
            const lat1 = roundCoordinate(record1.latitude);
            const lon1 = roundCoordinate(record1.longitude);
            
            for (let j = 0; j < records2.length; j++) {
                // Пропускаем уже обработанные записи
                if (processedIndices.has(j)) continue;
                
                const record2 = records2[j];
                
                if (!record2.latitude || !record2.longitude) continue;
                
                // Проверяем совпадение координат
                if (lat1 === records2Lat[j] && lon1 === records2Lon[j]) {
                    // Проверяем совпадение времени
                    if (isTimeMatch(record1.startTime, record2.startTime)) {
                        const timeDiff = Math.abs(new Date(record1.startTime) - new Date(record2.startTime)) / (1000 * 60);
                        
                        matches.push({
                            record1: record1,
                            record2: record2,
                            commonCoordinates: { 
                                latitude: lat1, 
                                longitude: lon1 
                            },
                            timeDifferenceMinutes: timeDiff
                        });
                        
                        processedIndices.add(j);
                        break; // Прерываем внутренний цикл после нахождения совпадения
                    }
                }
            }
        }
        
        return matches;
        
    } catch (error) {
        console.error('Ошибка при обработке файлов:', error);
        throw error;
    }
}

// Функция для создания Map для быстрого поиска (еще более производительный вариант)
async function findMatchingRecordsOptimized() {
    try {
        console.log('Чтение файлов (оптимизированный режим)...');
        const records1 = await parseCSV('хронология1.csv');
        const records2 = await parseCSV('хронология2.csv');
        
        console.log(`Загружено записей: хронология1.csv - ${records1.length}, хронология2.csv - ${records2.length}`);
        
        const matches = [];
        
        // Создаем Map для быстрого поиска по координатам
        const coordMap = new Map();
        
        // Заполняем Map записями из второго файла
        for (let i = 0; i < records2.length; i++) {
            const record2 = records2[i];
            if (!record2.latitude || !record2.longitude) continue;
            
            const lat = roundCoordinate(record2.latitude);
            const lon = roundCoordinate(record2.longitude);
            const key = `${lat},${lon}`;
            
            if (!coordMap.has(key)) {
                coordMap.set(key, []);
            }
            coordMap.get(key).push({
                record: record2,
                index: i
            });
        }
        
        // Ищем совпадения
        for (let i = 0; i < records1.length; i++) {
            const record1 = records1[i];
            if (!record1.latitude || !record1.longitude) continue;
            
            const lat1 = roundCoordinate(record1.latitude);
            const lon1 = roundCoordinate(record1.longitude);
            const key = `${lat1},${lon1}`;
            
            if (coordMap.has(key)) {
                const matches2 = coordMap.get(key);
                
                for (let j = 0; j < matches2.length; j++) {
                    const match2 = matches2[j];
                    const record2 = match2.record;
                    
                    if (isTimeMatch(record1.startTime, record2.startTime)) {
                        const timeDiff = Math.abs(new Date(record1.startTime) - new Date(record2.startTime)) / (1000 * 60);
                        
                        matches.push({
                            record1: record1,
                            record2: record2,
                            commonCoordinates: { 
                                latitude: lat1, 
                                longitude: lon1 
                            },
                            timeDifferenceMinutes: timeDiff
                        });
                        
                        // Удаляем использованную запись, чтобы не было повторений
                        coordMap.get(key).splice(j, 1);
                        j--;
                        
                        break; // Прерываем после нахождения совпадения
                    }
                }
            }
        }
        
        return matches;
        
    } catch (error) {
        console.error('Ошибка при обработке файлов:', error);
        throw error;
    }
}

// Функция для вывода результатов в консоль и сохранения в JSON
async function displayMatches() {
    try {
        // Используем оптимизированную версию для большей производительности
        const matches = await findMatchingRecordsOptimized();
        
        console.log(`\nНайдено совпадений: ${matches.length}\n`);
        
        // Формируем объект с результатами
        const result = {
            summary: {
                totalMatches: matches.length,
                timestamp: new Date().toISOString(),
                sourceFiles: {
                    file1: 'хронология1.csv',
                    file2: 'хронология2.csv'
                }
            },
            matches: new Array(matches.length)
        };
        
        // Заполняем массив matches
        for (let i = 0; i < matches.length; i++) {
            const match = matches[i];
            result.matches[i] = {
                matchNumber: i + 1,
                commonCoordinates: match.commonCoordinates,
                timeDifferenceMinutes: match.timeDifferenceMinutes,
                record1: {
                    startTime: match.record1.startTime,
                    endTime: match.record1.endTime,
                    probability: match.record1.probability || null,
                    latitude: match.record1.latitude,
                    longitude: match.record1.longitude,
                    source: match.record1.source
                },
                record2: {
                    startTime: match.record2.startTime,
                    endTime: match.record2.endTime,
                    probability: match.record2.probability || null,
                    latitude: match.record2.latitude,
                    longitude: match.record2.longitude,
                    source: match.record2.source
                }
            };
        }
        
        // Выводим в консоль
        console.log('Результаты поиска совпадений:');
        console.log(JSON.stringify(result, null, 2));
        
        // Сохраняем в JSON файл
        await saveMatchesToJSON(result);
        
        return result;
        
    } catch (error) {
        console.error('Ошибка:', error);
    }
}

// Функция для сохранения результатов в JSON
async function saveMatchesToJSON(data, outputPath = 'matches.json') {
    try {
        await fs.writeFile(outputPath, JSON.stringify(data, null, 2));
        console.log(`\nРезультаты сохранены в файл: ${outputPath}`);
    } catch (error) {
        console.error('Ошибка при сохранении JSON:', error);
    }
}

// Альтернативная функция для сохранения в JSON (простая версия)
async function saveMatchesToJSONSimple(matches, outputPath = 'matches_simple.json') {
    try {
        const simpleMatches = new Array(matches.length);
        
        for (let i = 0; i < matches.length; i++) {
            const match = matches[i];
            simpleMatches[i] = {
                latitude: match.commonCoordinates.latitude,
                longitude: match.commonCoordinates.longitude,
                time1: match.record1.startTime,
                time2: match.record2.startTime,
                diffMinutes: match.timeDifferenceMinutes,
                source1: match.record1.source,
                source2: match.record2.source
            };
        }
        
        await fs.writeFile(outputPath, JSON.stringify(simpleMatches, null, 2));
        console.log(`Упрощенные результаты сохранены в файл: ${outputPath}`);
    } catch (error) {
        console.error('Ошибка при сохранении упрощенного JSON:', error);
    }
}

// Запуск программы
displayMatches().catch(console.error);

// Экспорт функций
module.exports = {
    findMatchingRecords,
    findMatchingRecordsOptimized,
    displayMatches,
    saveMatchesToJSON,
    saveMatchesToJSONSimple,
    parseCSV
};