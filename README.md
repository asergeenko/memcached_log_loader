# memcached log loader
Многопоточная реализация загрузчика логов в memcached

### Пример строки лог-файла
idfa&nbsp;&nbsp;&nbsp;&nbsp;1rfw452y52g2gq4g&nbsp;&nbsp;&nbsp;&nbsp;55.5542.42&nbsp;&nbsp;&nbsp;&nbsp;1423,43,567,3,7,23

idfa&nbsp;&nbsp;&nbsp;&nbsp;2rfw452y52g2gq4g&nbsp;&nbsp;&nbsp;&nbsp;55.55&nbsp;&nbsp;&nbsp;&nbsp;42.42&nbsp;&nbsp;&nbsp;&nbsp;2423,43,567,3,7,23

idfa&nbsp;&nbsp;&nbsp;&nbsp;3rfw452y52g2gq4g&nbsp;&nbsp;&nbsp;&nbsp;55.55&nbsp;&nbsp;&nbsp;&nbsp;42.42&nbsp;&nbsp;&nbsp;&nbsp;3423,43,567,3,7,23

### Архитектура

Загрузчик реализован с помощью примитивов синхронизации из модуля [threading](https://docs.python.org/3/library/threading.html#module-threading) и модуля [queue](https://docs.python.org/3/library/queue.html#module-queue), а также пула потоков *Pool* из модуля [multiprocessing.dummy](https://docs.python.org/3/library/multiprocessing.html)  

## Запуск
```memc_load_thread [-t] [-w WORKERS] [-l LOG] [--dry] [--idfa] [--gaid] [--adid] [--dvid]```

### Параметры

- *-t* - тестовый запуск 

- *WORKERS* - количество worker'ов (по умолчанию 5)

- *LOG* - лог-файл

- *--dry* - парсинг лог-файла без записи в memcached

- *--idfa*, *--gaid*, *--adid*, *--dvid* - строка подключения к memcached (хост:порт) для соответствующего типа устройства

## Тестирование кода
``` pytest tests ```

## Требования
Python >= 3.6
protobuf
pymemcache