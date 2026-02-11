const fs = require('fs').promises;
const path = require('path');

// Функция для парсинга CSV
async function parseCSV(filePath) {
    const content = await fs.readFile(filePath, 'utf8');
    const lines = content.trim().split('\n');
    
    // Парсим заголовки (первая строка)
    const headerLine = lines[0];
    const headers = headerLine.split(',').map(header => header.trim());
    
    return lines.slice(1).map(line => {
        // Парсим строку с учетом кавычек
        const values = [];
        let currentValue = '';
        let insideQuotes = false;
        
        for (let i = 0; i < line.length; i++) {
            const char = line[i];
            
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
        const cleanValues = values.map(value => {
            if (value.startsWith('"') && value.endsWith('"')) {
                return value.slice(1, -1);
            }
            return value;
        });
        
        const record = {};
        headers.forEach((header, index) => {
            record[header] = cleanValues[index] || '';
        });
        
        return record;
    });
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

// Основная функция для поиска совпадений
async function findMatchingRecords() {
    try {
        // Читаем оба файла
        console.log('Чтение файлов...');
        const records1 = await parseCSV('хронология1.csv');
        const records2 = await parseCSV('хронология2.csv');
        
        console.log(`Загружено записей: хронология1.csv - ${records1.length}, хронология2.csv - ${records2.length}`);
        
        const matches = [];
        const processedIndices = new Set();
        
        // Ищем совпадения
        records1.forEach((record1, index1) => {
            if (!record1.latitude || !record1.longitude) return;
            
            const lat1 = roundCoordinate(record1.latitude);
            const lon1 = roundCoordinate(record1.longitude);
            
            records2.forEach((record2, index2) => {
                // Пропускаем уже обработанные записи
                if (processedIndices.has(index2)) return;
                
                if (!record2.latitude || !record2.longitude) return;
                
                const lat2 = roundCoordinate(record2.latitude);
                const lon2 = roundCoordinate(record2.longitude);
                
                // Проверяем совпадение координат
                if (lat1 === lat2 && lon1 === lon2) {
                    // Проверяем совпадение времени
                    if (isTimeMatch(record1.startTime, record2.startTime)) {
                        matches.push({
                            record1: record1,
                            record2: record2,
                            commonCoordinates: { 
                                latitude: lat1, 
                                longitude: lon1 
                            },
                            timeDifferenceMinutes: Math.abs(new Date(record1.startTime) - new Date(record2.startTime)) / (1000 * 60)
                        });
                        processedIndices.add(index2);
                    }
                }
            });
        });
        
        return matches;
        
    } catch (error) {
        console.error('Ошибка при обработке файлов:', error);
        throw error;
    }
}

// Функция для вывода результатов в консоль и сохранения в JSON
async function displayMatches() {
    try {
        const matches = await findMatchingRecords();
        
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
            matches: matches.map((match, index) => ({
                matchNumber: index + 1,
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
            }))
        };
        
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
        const simpleMatches = matches.map(match => ({
            latitude: match.commonCoordinates.latitude,
            longitude: match.commonCoordinates.longitude,
            time1: match.record1.startTime,
            time2: match.record2.startTime,
            diffMinutes: match.timeDifferenceMinutes,
            source1: match.record1.source,
            source2: match.record2.source
        }));
        
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
    displayMatches,
    saveMatchesToJSON,
    saveMatchesToJSONSimple,
    parseCSV
};