# Локальний Kafka для навчання

Встановіть Java 17 і завантажте Kafka 3.9.1 з [офіційного архіву Apache](https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz). Розпакуйте архів. Далі `KAFKA_HOME` — абсолютний шлях до розпакованого каталогу, а `JAVA_HOME` — шлях до JDK 17.

З каталогу цього проєкту виконайте один раз:

```sh
mkdir -p .local-kafka
"$KAFKA_HOME/bin/kafka-storage.sh" random-uuid
```

Скопіюйте отриманий UUID у наступну команду замість `YOUR_CLUSTER_UUID`:

```sh
"$KAFKA_HOME/bin/kafka-storage.sh" format -t YOUR_CLUSTER_UUID -c config/kraft.properties
"$KAFKA_HOME/bin/kafka-server-start.sh" config/kraft.properties
```

Не форматуйте сховище повторно для вже створеного кластера. Брокер працює у своєму терміналі до Ctrl+C. Для наступного запуску потрібна тільки остання команда. Порт 19092 має бути вільним. Конфігурація призначена для одного навчального брокера на тому самому комп’ютері; не відкривайте ці порти в інтернет.
