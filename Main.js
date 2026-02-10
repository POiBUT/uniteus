const fs = require("fs").promises;
const XLSX = require("xlsx");
const path = require("path");

// Функция для парсинга координат
function parseLatLng(latLngString) {
  if (!latLngString) return { latitude: "", longitude: "" };

  const parts = latLngString
    .replace(/°/g, "")
    .split(",")
    .map((s) => s.trim());
  return parts.length == 2
    ? { latitude: parts[0], longitude: parts[1] }
    : { latitude: "", longitude: "" };
}

// Асинхронная обработка файла
async function processJsonFileAsync(filePath) {
  try {
    console.log(`Чтение JSON файла: ${filePath}`);

    const rawData = await fs.readFile(filePath, "utf8");
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
            startTime: segment.startTime || "",
            endTime: segment.endTime || "",
            probability: activity.topCandidate?.probability || 0.0,
            latitude,
            longitude,
            source: `activity.start.${activity.topCandidate?.type}`,
          });
        }

        if (activity.end?.latLng) {
          const { latitude, longitude } = parseLatLng(activity.end.latLng);
          rows.push({
            startTime: segment.startTime || "",
            endTime: segment.endTime || "",
            probability: activity.topCandidate?.probability || 0.0,
            latitude,
            longitude,
            source: `activity.end.${activity.topCandidate?.type}`,
          });
        }
      }

      // Обработка visit
      else if (segment.visit) {
        const visit = segment.visit;

        if (visit.topCandidate?.placeLocation?.latLng) {
          const { latitude, longitude } = parseLatLng(
            visit.topCandidate.placeLocation.latLng,
          );
          rows.push({
            startTime: segment.startTime || "",
            endTime: segment.endTime || "",
            probability: visit.probability || 0.0,
            latitude,
            longitude,
            source: `visit.${visit.topCandidate.semanticType}`,
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
              probability: "",
              latitude,
              longitude,
              source: "timelinePath",
            });
          }
        }
      }
    }

    console.log(`Обработано ${rows.length} записей`);
    return rows;
  } catch (error) {
    console.error("Ошибка при обработке файла:", error.message);
    throw error;
  }
}

// Безопасная функция для нахождения минимума и максимума дат
function getMinMaxDatesSafe(dates) {
  if (dates.length === 0) return { min: null, max: null };

  let min = dates[0];
  let max = dates[0];

  // Используем цикл вместо spread оператора
  for (let i = 1; i < dates.length; i++) {
    const date = dates[i];
    if (date < min) min = date;
    if (date > max) max = date;
  }

  return { min, max };
}

// Исправленная генерация статистики
function generateStatistics(rows) {
  const stats = [];

  // Основная статистика
  stats.push({ Параметр: "Всего записей", Значение: rows.length });

  if (rows.length === 0) {
    stats.push({ Параметр: "--- Инфо ---", Значение: "" });
    stats.push({
      Параметр: "Сгенерировано",
      Значение: new Date().toLocaleString(),
    });
    return stats;
  }

  // Статистика по источникам
  const sourceCounts = {};
  // Используем цикл вместо forEach для больших массивов
  for (let i = 0; i < rows.length; i++) {
    const source = rows[i].source;
    sourceCounts[source] = (sourceCounts[source] || 0) + 1;
  }

  stats.push({ Параметр: "--- По источникам ---", Значение: "" });

  const sources = Object.keys(sourceCounts);
  for (let i = 0; i < sources.length; i++) {
    const source = sources[i];
    const count = sourceCounts[source];
    const percentage = ((count / rows.length) * 100).toFixed(1);
    stats.push({
      Параметр: source,
      Значение: `${count} (${percentage}%)`,
    });
  }

  // Статистика по координатам
  let validCoords = 0;
  for (let i = 0; i < rows.length; i++) {
    if (rows[i].latitude && rows[i].longitude) {
      validCoords++;
    }
  }

  stats.push({ Параметр: "--- Координаты ---", Значение: "" });
  stats.push({ Параметр: "С валидными координатами", Значение: validCoords });
  stats.push({
    Параметр: "Без координат",
    Значение: rows.length - validCoords,
  });

  // Временной диапазон
  const times = [];
  for (let i = 0; i < rows.length; i++) {
    const date = new Date(rows[i].startTime);
    if (!isNaN(date.getTime())) {
      times.push(date);

      // Ограничим количество дат для обработки
      if (times.length > 100000) {
        console.log(
          "Предупреждение: ограничение обработки временных меток для производительности",
        );
        break;
      }
    }
  }

  if (times.length > 0) {
    const { min: minTime, max: maxTime } = getMinMaxDatesSafe(times);
    stats.push({ Параметр: "--- Время ---", Значение: "" });
    stats.push({ Параметр: "Начало", Значение: minTime.toLocaleString() });
    stats.push({ Параметр: "Конец", Значение: maxTime.toLocaleString() });
  }

  // Дата генерации
  stats.push({ Параметр: "--- Инфо ---", Значение: "" });
  stats.push({
    Параметр: "Сгенерировано",
    Значение: new Date().toLocaleString(),
  });

  return stats;
}

// Упрощенная генерация статистики (альтернатива)
function generateStatisticsSimple(rows) {
  const stats = [];

  stats.push({ Параметр: "Всего записей", Значение: rows.length });

  if (rows.length === 0) {
    return stats;
  }

  // Статистика по источникам (упрощенная)
  const sourceCounts = {};
  let validCoords = 0;
  let earliestTime = null;
  let latestTime = null;

  // Один проход по данным
  for (let i = 0; i < Math.min(rows.length, 100000); i++) {
    // Ограничиваем для больших файлов
    const row = rows[i];

    // Подсчет источников
    sourceCounts[row.source] = (sourceCounts[row.source] || 0) + 1;

    // Подсчет валидных координат
    if (row.latitude && row.longitude) {
      validCoords++;
    }

    // Время (только для первых 10000 записей для производительности)
    if (i < 10000) {
      try {
        const date = new Date(row.startTime);
        if (!isNaN(date.getTime())) {
          if (!earliestTime || date < earliestTime) earliestTime = date;
          if (!latestTime || date > latestTime) latestTime = date;
        }
      } catch (e) {
        // Игнорируем ошибки парсинга дат
      }
    }
  }

  // Добавляем статистику по источникам
  stats.push({ Параметр: "--- По источникам ---", Значение: "" });
  Object.entries(sourceCounts).forEach(([source, count]) => {
    const percentage = ((count / rows.length) * 100).toFixed(1);
    stats.push({
      Параметр: source,
      Значение: `${count} (${percentage}%)`,
    });
  });

  stats.push({ Параметр: "--- Координаты ---", Значение: "" });
  stats.push({ Параметр: "С валидными координатами", Значение: validCoords });
  stats.push({
    Параметр: "Без координат",
    Значение: rows.length - validCoords,
  });

  if (earliestTime && latestTime) {
    stats.push({ Параметр: "--- Время (первые 10000) ---", Значение: "" });
    stats.push({ Параметр: "Начало", Значение: earliestTime.toLocaleString() });
    stats.push({ Параметр: "Конец", Значение: latestTime.toLocaleString() });
  }

  stats.push({ Параметр: "--- Инфо ---", Значение: "" });
  stats.push({
    Параметр: "Сгенерировано",
    Значение: new Date().toLocaleString(),
  });

  return stats;
}

// Сохранение в несколько форматов с оптимизацией для больших файлов
async function saveToMultipleFormats(rows, baseName) {
  const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, "-");
  const results = {};

  console.log("Начинаю сохранение результатов...");

  // CSV (если данных много)
  const maxCsvRowNumber = 50000000;
  if (rows.length <= maxCsvRowNumber) {
    console.log("Создание CSV файла...");
    const csvFile = `${baseName}_${timestamp}.csv`;
    // Записываем постепенно для больших файлов
    const writeStream = require("fs").createWriteStream(csvFile, {
      encoding: "utf8",
    });
    writeStream.write(
      "startTime,endTime,probability,latitude,longitude,source\n",
    );

    const batchSize = 10000;
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      const csvBatch =
        batch
          .map(
            (row) =>
              `"${(row.startTime || "").replace(/"/g, '""')}","${(row.endTime || "").replace(/"/g, '""')}",${row.probability || ""},"${row.latitude}","${row.longitude}","${row.source}"`,
          )
          .join("\n") + (i + batchSize < rows.length ? "\n" : "");

      writeStream.write(csvBatch);

      if ((i >= batchSize && i % (100000 - batchSize) === 0) || i + batchSize >= rows.length) {
        console.log(
          `  CSV: записано ${Math.min(i + batchSize, rows.length)} из ${rows.length} строк`,
        );
      }
    }

    await new Promise((resolve) => {
      writeStream.end(resolve);
    });
    results.csv = csvFile;
    console.log(`CSV файл сохранен: ${csvFile}`);
  } else {
    console.log(`Пропускаю создание CSV (слишком много данных: ${rows.length} строк, максимум: ${maxCsvRowNumber} строк)`);
  }

  return results;
}

// Основная функция с обработкой ошибок памяти
async function main() {
  try {
    const inputFile = process.argv[2] || "хронология2.json";
    const outputBase = process.argv[3] || "хронология2";

    console.log(`=== Обработка файла: ${inputFile} ===\n`);

    // Проверяем существование файла
    try {
      await fs.access(inputFile);
    } catch {
      console.error(`❌ Файл "${inputFile}" не найден!`);
      console.log(
        "Использование: node script.js [входной.json] [префикс_выходного]",
      );
      process.exit(1);
    }

    // Увеличиваем лимит стека при необходимости
    if (process.argv.includes("--increase-stack")) {
      const v8 = require("v8");
      v8.setFlagsFromString("--stack-size=2000");
      console.log("Увеличен лимит стека");
    }

    // Обрабатываем файл
    console.log("Обработка данных...");
    const rows = await processJsonFileAsync(inputFile);

    if (rows.length === 0) {
      console.log("⚠️  Нет данных для обработки.");
      return;
    }

    console.log(`\n✅ Обработано ${rows.length} записей\n`);

    // Сохраняем в несколько форматов
    console.log("💾 Сохранение результатов...");
    const savedFiles = await saveToMultipleFormats(rows, outputBase);

    console.log("\n✅ Результаты сохранены:");
    if (savedFiles.csv) console.log(`📄 CSV:   ${savedFiles.csv}`);

    // Выводим предпросмотр
    if (rows.length <= 10) {
      console.log("\n👀 Предпросмотр всех строк:");
      console.table(rows);
    } else {
      console.log("\n👀 Предпросмотр первых 3 строк:");
      console.table(rows.slice(0, 3));
    }

    // generateStatisticsSimple(rows);
    // generateStatistics(rows);
  } catch (error) {
    console.error("\n❌ Ошибка:", error.message);
    if (error.message.includes("stack") || error.message.includes("memory")) {
      console.log(
        "\n💡 Совет: Попробуйте запустить с увеличенным лимитом памяти:",
      );
      console.log("node --max-old-space-size=4096 script.js файл.json");
    }
    process.exit(1);
  }
}

// Экспорт функций
module.exports = {
  processJsonFileAsync,
  parseLatLng,
  generateStatisticsSimple,
  getMinMaxDatesSafe,
};

// Запуск
if (require.main === module) {
  // Увеличиваем лимит памяти при необходимости
  if (process.argv.includes("--memory")) {
    console.log("Используется увеличенный лимит памяти");
  }

  main();
}
