# Informe

## Coordinación entre instancias de Sum
Todas las instancias de `Sum` consumen de una misma cola de trabajo `input_queue`. Debido a que el `Gateway` publica un único `EOF` a dicha cola, solamente una instancia de `Sum` la recibirá, por lo que fue necesario implementar un método de sincronización.

Se optó por un **coordinador por ronda**: en cada ronda hay una instancia de `Sum` que actúa como coordinador del cliente. No hay coordinador fijo ni ningún tipo de mecanismo de elección de coordinador. El recibimiento del único `EOF` es quien le otorga el rol de coordinador a una instancia.

Por un lado se manejan los datos y por otro el control. Los datos llegan por una cola compartida `input_queue`, mientras que el control usa un exchange propio de tipo `direct` con dos *keys*:
- `control_broadcast`: a la que se bindean todos los `Sum`, utilizada para difundir.
- `control_node_<id>`: a la que se bindea un `Sum` para enviarle un mensaje al coordinador de la ronda.

### Protocolo
La fase de cada ronda se identifica por medio de una *flag* incluida en un `ControlToken`, el cual contiene además los campos `client_id`, `coordinator_id` y `current_count`.
1. **Elección**: El `Sum` que recibió el `EOF` del Gateway se declara coordinador de la ronda y calcula cuántos mensajes le falta procesar de los demás nodos `Sum` utilizando el campo de total de mensajes que viene adjunto al `EOF`. A partir de ahí, envía sus propios resultados parciales y su `EOF` a los `Aggregator`. Luego, envía un `ControlToken` con la *flag* `COORD` a los demás nodos `Sum` mediante el exchange de *broadcast*.
2. **Conteo**: Cada `Sum` recibe el `COORD`, registra quién es el coordinador de la ronda, lee su propio contador, lo resetea y le devuelve un token con la *flag* `COUNT`, a modo de informarle cuántos mensajes procesó para el cliente en cuestión. El coordinador descuenta ese número.
3. **Cierre**: Cuando el coordinador registra que no hay más mensajes pendientes, difunde un token con la *flag* `FINAL_ROUND` indicando que los nodos `Sum` pueden vaciar el estado del cliente en cuestión y emitir sus resultados. 

## Coordinación entre instancias de Aggregation
La coordinación en esta etapa se da mediante una **barrera de sincronización**. No hay comunicación directa entre los `Aggregators`.
1. **Sharding**: Los datos que provienen de las instancias de `Sum` pasan por una función de hasheo que utiliza la fruta y el cliente.
2. **Barrera por EOF**: Cada `Aggregator` necesita saber cuándo terminar su porción del trabajo, así que se aprovechó su conocimiento de la cantidad de nodos `Sum` (el dato `SumAmount`) para que cada `Aggregator` lleve registro de cuántos `EOF` recibió. Cuando un `Sum` termina su parte, publica su `EOF` a todas las *keys* del exchange de modo que cada `Aggregator` recibe un `EOF` por `Sum`.
3. **Pase de fase**: Únicamente cuando un `Aggregator` recibió una cantidad `SumAmount` de `EOF` para un cliente, deduce que ningún otro `Sum` le enviará datos y entonces procesará su top parcial y lo enviará al `Join`, sumado a su propio `EOF`. `Join` opera de la misma manera, esperando una cantidad de `EOF` igual a `AggregationAmount`, y luego fusiona los tops para emitir el top final.

## Escalabilidad
- **Respecto a los clientes**: Como el `Gateway` le asigna a cada cliente un `clientID` único, todas las estructuras (`Sum`, `Aggregator`, `Join`) se particionan usando `clientID` como clave, permitiendo procesamiento en simultáneo.
- **Respecto a grandes volúmenes de datos**: Tanto en `Sum` como en `Aggregator` se usan diccionarios que se modifican a medida que llegan nuevos datos. El uso de memoria solo aumenta en función de la cantidad de frutas únicas que se agregan y no en función de la cantidad de mensajes. Además, el nodo `Join` realiza una fusión de datos que descarta los elementos sobrantes y se asegura de almacenar únicamente `TOP_SIZE` elementos por cliente.
- **Respecto a la cantidad de controles**: En el caso de un cuello de botella en la ingesta de datos, se espera que RabbitMQ balancee automáticamente la carga en esa cola de entrada. Si la consolidación de datos es lenta, se pueden agregar instancias de `Aggregation` sin problema ya que el *sharding* y la barrera de sincronización se adaptan automáticamente a ese aumento en la topología.
