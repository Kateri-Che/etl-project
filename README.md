## Построение ETL-пайплайна

### Контекст
Требуется настроить автоматическую выгрузку показателей по определенным метрикам в отдельную таблицу в базе данных.

### Задача проекта
Требуется написать DAG, который будет считать метрики по новостной ленте (feed_actions) и ленте сообщений (message_actions) каждый день за вчера. 
Каждая выгрузка должна быть в отдельном таске. 
Данные автоматически должны подгружаться в отдельную таблицу в Clickhouse.

### Стек
 - airflow, pandas, pandahouse, clickhouse, numpy, os

### Этапы реализации задачи ([посмотреть код](https://github.com/Kateri-Che/etl-project/blob/main/etl_dag.py))
**1. Подсчет метрик**:

**1.1.** Из таблицы **feed_actions** для каждого пользователя посчитать:

- количество просмотров (views).
- количество лайков (likes).

**Реализовано в таске `total_lv`**.

**Пример датасета после выполнения таски**:

![dataset:](https://github.com/Kateri-Che/etl-project/blob/main/sceenshots/total_lv.png)


**1.2.** Из таблицы **message_actions** для каждого пользователя посчитать:  
    
- количество отправленных сообщений (messages_sent).
- количество полученных сообщений (messages_received).
- количество пользователей, которым отправлены сообщения (users_sent).
- количество пользователей, от которых получены сообщения (users_received).

**Реализовано в таске `total_m`**.

**Пример датасета после выполнения таски**:

![dataset:](https://github.com/Kateri-Che/etl-project/blob/main/sceenshots/total_m.png)

**2.** Объединение таблиц в одну.

**Реализовано в таске `df_merging`**.

**Пример датасета после выполнения таски**:

![dataset:](https://github.com/Kateri-Che/etl-project/blob/main/sceenshots/df_merging.png)

**3.** Подсчет всеx метрик в разрезе по:
        
- полу (gender).
- возрасту (age).
- операционной системе (os).

**Реализовано в тасках `to_gender`, `to_age`, `to_os`**.

**Пример датасетов после выполнения тасок**:

![dataset:](https://github.com/Kateri-Che/etl-project/blob/main/sceenshots/to_gender.png)
![dataset:](https://github.com/Kateri-Che/etl-project/blob/main/sceenshots/to_age.png)
![dataset:](https://github.com/Kateri-Che/etl-project/blob/main/sceenshots/to_os.png)
   
**4.** Загрузка данных:

   **4.1**. Создание таблицы для загрузки данных со следующей структурой:
   - `event_date` - дата;
   - `dimension` - наименование среза (гендер, пол или операционная система);
   - `dimension_value` - значение среза;
   - `views` - количество просмотров;
   - `likes` - количество лайков;
   - `messages_sent` - количество отправленных сообщений;
   - `messages_received` - количество полученных сообщений;
   - `users_sent` - количество пользователей, которым отправлены сообщения;
   - `users_received` - количество пользователей, от которых получены сообщения.
     
   **4.2**. Oбъединение датасетов, форматирование типов данных (при необходимости).
   
   **4.3**. Загрузка данных в финальную таблицу в Clickhouse.

**Реализовано в таске `load`**.

### Результат
Результатом выполнения проекта стало создание DAG-а, который:
 - реализует ежедневный подсчет ключевых метрик по двум сервисам;
 - обеспечивает актуальность информации и позволяет своевременно получать доступ к ежедневной статистике для последующего анализа.

**Граф в Airflow**:

![dag:](https://github.com/Kateri-Che/etl-project/blob/main/sceenshots/dag.png)

**Финальная таблица в Clickhouse**:

![table:](https://github.com/Kateri-Che/etl-project/blob/main/sceenshots/clickhouse_data.png)

**Загруженные в финальную таблицу данные за несколько дней: [скачать пример](https://drive.google.com/drive/folders/1Ru9pKkihgfE1skJMVboEylY8ZgdmXOud?usp=sharing)**
