# Event Driven Architecture & Real Time Stream Processing avec Apache Kafka, Spring Cloud Stream et Kafka Streams

Ce projet est une application **Spring Boot** qui illustre, de bout en bout, la mise en œuvre d'une **architecture orientée événements (Event Driven Architecture)** et du **traitement de flux en temps réel (Real Time Stream Processing)** avec **Apache Kafka**, **Spring Cloud Stream** et **Kafka Streams**.

Il s'inspire du tutoriel *« Event Driven Architecture – Spring Cloud Streams – KAFKA »* de **Mohamed Youssfi** ([vidéo YouTube](https://www.youtube.com/watch?v=8uY7JE_X_Fw)) et reprend le code du dépôt [`kafka-spring-cloud-stream`](https://github.com/mohamedYoussfi/kafka-spring-cloud-stream).

L'application manipule des évènements de type **`PageEvent`** (visites de pages web) et démontre les principaux modèles de programmation de Spring Cloud Stream : **Producer**, **Consumer**, **Supplier** et **Function** (Kafka Streams), le tout couronné par une **application web temps réel** affichant les statistiques calculées par le moteur de stream processing.

![Architecture](captures/img.png)

---

## Sommaire

1. [Télécharger Kafka](#1-télécharger-kafka)
2. [Démarrer Zookeeper](#2-démarrer-zookeeper)
3. [Démarrer Kafka-server](#3-démarrer-kafka-server)
4. [Tester avec kafka-console-producer et kafka-console-consumer](#4-tester-avec-kafka-console-producer-et-kafka-console-consumer)
5. [Un Service Producer Kafka via un Rest Controller](#5-un-service-producer-kafka-via-un-rest-controller)
6. [Un Service Consumer Kafka](#6-un-service-consumer-kafka)
7. [Un Service Supplier Kafka](#7-un-service-supplier-kafka)
8. [Un Service de Data Analytics Real Time Stream Processing avec Kafka Streams](#8-un-service-de-data-analytics-real-time-stream-processing-avec-kafka-streams)
9. [Une application web qui affiche les résultats du Stream Data Analytics en temps réel](#9-une-application-web-qui-affiche-les-résultats-du-stream-data-analytics-en-temps-réel)

---

## Technologies utilisées

| Composant | Version                 |
|-----------|-------------------------|
| Java | 21                      |
| Spring Boot | 4.0.6                   |
| Spring Cloud | 2021.0.5                |
| Spring Cloud Stream Binder Kafka / Kafka Streams | —                       |
| Apache Kafka Streams | —                       |
| Lombok | —                       |
| Maven | wrapper inclus (`mvnw`) |

L'application démarre par défaut sur le port **8090** (voir `src/main/resources/application.yml`).

---

## Modèle de données : `PageEvent`

L'évènement échangé sur Kafka représente la consultation d'une page :

```java
public class PageEvent {
    private String name;     // nom de la page (ex : P1, P2)
    private String user;     // utilisateur (ex : U1, U2)
    private Date date;       // date de l'évènement
    private long duration;   // durée de consultation
}
```

---

## 1. Télécharger Kafka

Deux possibilités pour disposer d'un broker Kafka : l'installation binaire, ou Docker.

### Option A — Installation binaire

Téléchargez la dernière distribution depuis le site officiel : <https://kafka.apache.org/downloads>

Décompressez l'archive dans un répertoire, par exemple `C:\kafka` (Windows) ou `/opt/kafka` (Linux/macOS). Toutes les commandes ci-dessous sont à exécuter depuis ce répertoire.

### Option B — Docker (recommandé)

Le projet fournit un fichier `docker-compose.yml` qui démarre Zookeeper et un broker Kafka (images Confluent `7.3.0`). C'est l'option la plus simple ; elle remplace à elle seule les étapes 2 et 3 :

```bash
docker-compose up -d
```

Le broker est alors exposé sur `localhost:9092`.

---

## 2. Démarrer Zookeeper

Zookeeper coordonne le cluster Kafka. Il doit être lancé **avant** le broker.

**Windows**
```bat
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

**Linux / macOS**
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Zookeeper écoute par défaut sur le port `2181`.

> Avec Docker (`docker-compose up -d`), Zookeeper est déjà démarré dans le conteneur `zookeeper` ; vous pouvez ignorer cette étape.

---

## 3. Démarrer Kafka-server

Le broker Kafka transporte les messages entre producteurs et consommateurs.

**Windows**
```bat
bin\windows\kafka-server-start.bat config\server.properties
```

**Linux / macOS**
```bash
bin/kafka-server-start.sh config/server.properties
```

Le broker écoute par défaut sur le port `9092`.

> Avec Docker, le broker tourne dans le conteneur `broker` et est déjà accessible sur `localhost:9092`.

---

## 4. Tester avec kafka-console-producer et kafka-console-consumer

Avant de coder l'application, on vérifie que le broker fonctionne en envoyant et lisant des messages depuis la ligne de commande, sur un topic (par exemple `R4`).

### Consumer (terminal 1)

**Windows**
```bat
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic R4 ^
  --property print.key=true --property print.value=true ^
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer ^
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

### Producer (terminal 2)

**Windows**
```bat
bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic R4
```

Tapez ensuite quelques messages dans le producer : ils s'affichent en temps réel dans le consumer.

### Équivalent avec Docker

```bash
# Consumer
docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic R2

# Producer
docker exec --interactive --tty broker kafka-console-producer --bootstrap-server broker:9092 --topic R2

# Lister les topics
docker exec --interactive --tty broker kafka-topics --bootstrap-server broker:9092 --list
```

---

## 5. Un Service Producer Kafka via un Rest Controller

Le producteur publie des `PageEvent` dans un topic Kafka **à la demande**, via une API REST. On utilise `StreamBridge`, qui permet d'envoyer un message vers une destination de manière dynamique (sans binding déclaré à l'avance).

Fichier : `src/main/java/net/youssfi/demospringkafka/web/PageEventController.java`

```java
@RestController
public class PageEventController {
    private final StreamBridge streamBridge;
    private final InteractiveQueryService interactiveQueryService;

    public PageEventController(StreamBridge streamBridge,
                               InteractiveQueryService interactiveQueryService) {
        this.streamBridge = streamBridge;
        this.interactiveQueryService = interactiveQueryService;
    }

    @GetMapping("publish/{topic}/{name}")
    public PageEvent publish(@PathVariable String name, @PathVariable String topic) {
        PageEvent pageEvent = new PageEvent();
        pageEvent.setName(name);
        pageEvent.setDate(new Date());
        pageEvent.setDuration(new Random().nextInt(1000));
        pageEvent.setUser(Math.random() > 0.5 ? "U1" : "U2");
        streamBridge.send(topic, pageEvent);   // publication dans le topic Kafka
        return pageEvent;
    }
}
```

**Test :** après avoir lancé l'application, ouvrez dans un navigateur :

```
http://localhost:8090/publish/R333/P1
```

Cela publie un évènement `PageEvent` (page `P1`) dans le topic `R333` et renvoie l'objet envoyé au format JSON.

---

## 6. Un Service Consumer Kafka

Le consommateur reçoit les `PageEvent` publiés dans un topic. Avec Spring Cloud Stream, il suffit de déclarer un bean de type `java.util.function.Consumer`.

Fichier : `src/main/java/net/youssfi/demospringkafka/service/PageEventService.java`

```java
@Configuration
public class PageEventService {

    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return (pageEvent -> {
            System.out.println("******------************");
            System.out.println(pageEvent.toString());
            System.out.println("*******-----**********");
        });
    }
}
```

Le binding associé est défini dans `application.yml` (la fonction écoute le topic `R224`, groupe `G1`) :

```yaml
spring.cloud.stream:
  function:
    definition: pageEventConsumer;pageEventSupplier;pageStreamConsumer;kStreamFunction2
  bindings:
    pageEventConsumer-in-0:
      destination: R224
      group: G1
```

> Convention de nommage : pour un bean `pageEventConsumer`, l'entrée est `pageEventConsumer-in-0`.

---

## 7. Un Service Supplier Kafka

Le `Supplier` est un **producteur automatique** : Spring Cloud Stream l'invoque périodiquement (par défaut toutes les secondes) pour générer et publier des évènements en continu. C'est ce qui alimente le pipeline de stream processing sans intervention manuelle.

Fichier : `src/main/java/net/youssfi/demospringkafka/service/PageEventService.java`

```java
@Bean
public Supplier<PageEvent> pageEventSupplier() {
    return () -> PageEvent.builder()
            .name((Math.random() > 0.5) ? "P1" : "P2")
            .user((Math.random() > 0.5) ? "U1" : "U2")
            .date(new Date())
            .duration(new Random().nextInt(1000))
            .build();
}
```

Le binding de sortie (`application.yml`) envoie ces évènements dans le topic `R333` :

```yaml
  bindings:
    pageEventSupplier-out-0:
      destination: R333
```

Ainsi, dès le démarrage de l'application, un flux régulier de `PageEvent` (pages `P1`/`P2`, utilisateurs `U1`/`U2`) est produit dans le topic `R333`.

---

## 8. Un Service de Data Analytics Real Time Stream Processing avec Kafka Streams

C'est le cœur du traitement temps réel. Une **fonction Kafka Streams** consomme le flux d'évènements, le transforme et calcule des agrégats sur des fenêtres de temps glissantes. Le résultat est stocké dans un **state store** interrogeable.

Fichier : `src/main/java/net/youssfi/demospringkafka/processors/StreamDataAnalyticService.java`

```java
@Service
public class StreamDataAnalyticService {

    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Double>> kStreamFunction2() {
        return (input) -> input
                .map((k, v) -> new KeyValue<>(v.getName(), (double) v.getDuration()))
                .groupBy((k, v) -> k, Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(30)))   // fenêtre de 30 s
                .aggregate(() -> 0.0,
                           (k, v, total) -> total + v,
                           Materialized.as("total-store"))             // state store
                .toStream()
                .map((k, v) -> new KeyValue<>(k.key().toString(), v));
    }
}
```

Cette fonction :
- lit le flux d'entrée (les `PageEvent` produits par le `Supplier`),
- regroupe les évènements par **nom de page**,
- calcule, sur des **fenêtres de 30 secondes**, la **somme cumulée des durées** par page,
- matérialise le résultat dans le state store `total-store`.

Bindings correspondants (`application.yml`) — entrée sur `R333`, sortie sur `R66` :

```yaml
  bindings:
    kStreamFunction2-in-0:
      destination: R333
      group: G44
    kStreamFunction2-out-0:
      destination: R66
```

Configuration Kafka Streams (`application.properties`) :

```properties
spring.kafka.streams.application-id=app2
spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=5000
spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde
spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde=org.apache.kafka.common.serialization.Serdes$DoubleSerde
```

### Exposition des résultats via Server-Sent Events (SSE)

Le contrôleur interroge le state store grâce à `InteractiveQueryService` et publie les statistiques sous forme de flux SSE consommable par le navigateur :

```java
@GetMapping(value = "/analyticsAggregate", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public Flux<Map<String, Double>> analyticsAggregate() {
    return Flux.interval(Duration.ofSeconds(1)).map(seq -> {
        Map<String, Double> map = new HashMap<>();
        ReadOnlyWindowStore<String, Double> stats =
                interactiveQueryService.getQueryableStore("total-store", QueryableStoreTypes.windowStore());
        Instant now = Instant.now();
        Instant from = now.minusSeconds(30);
        KeyValueIterator<Windowed<String>, Double> it = stats.fetchAll(from, now);
        while (it.hasNext()) {
            KeyValue<Windowed<String>, Double> next = it.next();
            map.put(next.key.key(), next.value);
        }
        return map;
    });
}
```

Trois endpoints d'analytics sont disponibles :

| Endpoint | State store | Description |
|----------|-------------|-------------|
| `GET /analytics` | `count-store` (KeyValueStore) | comptage par clé |
| `GET /analyticsWindows` | `count-store` (WindowStore) | comptage par fenêtre |
| `GET /analyticsAggregate` | `total-store` (WindowStore) | somme des durées par page sur fenêtre de 30 s |

---

## 9. Une application web qui affiche les résultats du Stream Data Analytics en temps réel

Une page HTML statique consomme le flux SSE `/analyticsAggregate` et trace une courbe en temps réel à l'aide de la bibliothèque **SmoothieCharts**.

Fichier : `src/main/resources/static/index.html`

```html
<canvas id="chart2" width="600" height="400"></canvas>
<script>
  var pages = ["P1", "P2"];
  var smoothieChart = new SmoothieChart({ tooltip: true });
  smoothieChart.streamTo(document.getElementById("chart2"), 500);

  var courbe = [];
  pages.forEach(function (v) {
    courbe[v] = new TimeSeries();
    smoothieChart.addTimeSeries(courbe[v], { lineWidth: 2 });
  });

  // Connexion au flux temps réel exposé par le service de stream processing
  var stockEventSource = new EventSource("/analyticsAggregate");
  stockEventSource.addEventListener("message", function (event) {
    pages.forEach(function (v) {
      var val = JSON.parse(event.data)[v];
      courbe[v].append(new Date().getTime(), val);
    });
  });
</script>
```

**Accès :** une fois l'application lancée, ouvrez :

```
http://localhost:8090/index.html
```

Vous voyez deux courbes (`P1` et `P2`) se mettre à jour en continu, reflétant la somme des durées de consultation calculée en temps réel par le pipeline Kafka Streams alimenté par le `Supplier`.

---

## Démarrage rapide (récapitulatif)

```bash
# 1. Démarrer Kafka (le plus simple : Docker)
docker-compose up -d

# 2. Compiler et lancer l'application Spring Boot
./mvnw spring-boot:run        # Linux / macOS
mvnw.cmd spring-boot:run      # Windows
```

Puis :
- Produire un évènement à la demande : <http://localhost:8090/publish/R333/P1>
- Visualiser les analytics temps réel : <http://localhost:8090/index.html>

---

## Structure du projet

```
kafka-spring-cloud-stream/
├── docker-compose.yml                 # Zookeeper + Broker Kafka (Confluent)
├── commands.txt                       # Commandes Kafka console & Docker
├── pom.xml
└── src/main/
    ├── java/net/youssfi/demospringkafka/
    │   ├── entities/PageEvent.java                 # modèle d'évènement
    │   ├── web/PageEventController.java            # Producer REST + endpoints SSE
    │   ├── service/PageEventService.java           # Consumer & Supplier
    │   ├── service/AppSerdes.java                  # Serde JSON pour PageEvent
    │   ├── processors/StreamDataAnalyticService.java  # Fonction Kafka Streams
    │   └── serializers/                            # Serializer/Deserializer custom
    └── resources/
        ├── application.yml                         # bindings Spring Cloud Stream
        ├── application.properties                  # config Kafka Streams
        └── static/index.html                       # application web temps réel
```

---

## Récapitulatif des topics et bindings

| Composant | Type | Topic (binding) |
|-----------|------|-----------------|
| `publish/{topic}/{name}` | Producer (StreamBridge) | dynamique (ex. `R333`) |
| `pageEventConsumer` | Consumer | `R224` (groupe `G1`) |
| `pageEventSupplier` | Supplier | → `R333` |
| `kStreamFunction2` | Function (Kafka Streams) | `R333` → `R66` |

---

## Auteur & source

- Tutoriel original : **Mohamed Youssfi** — [Event Driven Architecture – Spring Cloud Streams – KAFKA](https://www.youtube.com/watch?v=8uY7JE_X_Fw)
- Dépôt : <https://github.com/mohamedYoussfi/kafka-spring-cloud-stream>
