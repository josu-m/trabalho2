# Relatório – Trabalho Prático de Avaliação #2
## Comunicação e Sincronização entre Processos

**Universidade de Aveiro – Escola Superior de Tecnologia e Gestão de Águeda**  
**Unidade Curricular:** Sistemas Operativos / Programação Concorrente  

---

## 1. Arquitetura da Solução

### 1.1 Visão Geral

O sistema é composto por **quatro módulos Python** que cooperam através de memória partilhada gerida pelo `multiprocessing.Manager`:

```
main.py           ← ponto de entrada, cria memória partilhada e lança processos
├── airport.py    ← processo servidor (Aeroporto)
├── passenger.py  ← processo cliente (cada Passageiro)
├── shared_memory.py  ← estruturas de dados partilhadas e constantes
└── logger.py     ← registo de operações (ficheiro + consola)
```

### 1.2 Componentes Principais

#### Servidor – `Airport` (airport.py)
O servidor é um **processo independente** (`multiprocessing.Process`) que corre em loop contínuo:

1. Retira o passageiro com maior prioridade da fila
2. Aguarda um portão **e** um agente disponíveis (via semáforos)
3. Lança uma **thread** de embarque por cada passageiro atendido
4. Mantém uma **thread de vigilância** que detecta desistências em background

#### Passageiros – `passenger_process` (passenger.py)
Cada passageiro é um **processo independente** que:

1. Dorme o tempo de `delay` (simulando a chegada ao aeroporto)
2. Regista os seus dados no dicionário partilhado
3. Insere-se na fila de prioridade
4. Fica em polling leve até o seu estado mudar para `boarding`, `boarded` ou `abandoned`

### 1.3 Memória Partilhada

| Objeto partilhado | Tipo | Finalidade |
|---|---|---|
| `passengers` | `Manager.dict` | Dados completos de cada passageiro |
| `queue._heap` | `Manager.list` | Heap de prioridade da fila de embarque |
| `queue._lock` | `Manager.Lock` | Exclusão mútua na fila |
| `gates_state` | `Manager.list` | Estado livre/ocupado de cada portão |
| `agents_state` | `Manager.list` | Estado livre/ocupado de cada agente |
| `gate_sem` | `Manager.Semaphore` | Controlo de portões disponíveis |
| `agent_sem` | `Manager.Semaphore` | Controlo de agentes disponíveis |
| `stop_flag` | `Manager.Value` | Sinal de paragem do servidor |
| `stats` | `Manager.dict` | Estatísticas globais (embarcados, desistências) |

---

## 2. Mecanismos de Sincronização

### 2.1 Semáforos
- **`gate_sem = Semaphore(NUM_GATES)`** – garante que nunca se alocam mais portões do que os disponíveis. O servidor faz `acquire()` antes de alocar e `release()` após libertar.
- **`agent_sem = Semaphore(NUM_AGENTS)`** – idem para agentes de embarque.

### 2.2 Locks (Exclusão Mútua)
- **`queue._lock`** – protege todas as operações de leitura/escrita na heap da fila, evitando condições de corrida quando múltiplos processos tentam aceder simultaneamente.
- **`gates_lock` / `agents_lock`** – protegem a iteração sobre as listas de estado, garantindo que dois embarques não alocam o mesmo recurso.

### 2.3 Ausência de Condições de Corrida
A combinação semáforo + lock segue o padrão clássico:
1. `sem.acquire()` garante que existe recurso disponível
2. `lock.__enter__()` garante acesso exclusivo durante a seleção do recurso específico
3. Desta forma, nunca dois passageiros partilham o mesmo portão ou agente

---

## 3. Prioridade de Embarque

A prioridade é mapeada numericamente (menor número = maior prioridade):

| Classe | Prioridade | Tempo de Embarque |
|---|---|---|
| Primeira Classe | 1 | 2 segundos |
| Executiva | 2 | 3 segundos |
| Económica | 3 | 5 segundos |

A fila é implementada como um **min-heap** sobre `(prioridade, tempo_de_chegada, id)`. Assim, dentro do mesmo nível de prioridade, os passageiros são atendidos por ordem de chegada (FIFO).

---

## 4. Desistências

A thread `_check_abandonments` corre em background no servidor, verificando cada segundo se algum passageiro em estado `waiting` ultrapassou `MAX_WAIT` segundos. Quando detecta uma desistência:

1. Altera o estado do passageiro para `abandoned`
2. Remove-o da fila de prioridade (evitando que seja processado)
3. Regista o evento no log
4. Incrementa o contador de estatísticas

---

## 5. Cenários Suportados

| Cenário | Descrição | Comando |
|---|---|---|
| Normal | 12 passageiros com chegadas espaçadas | `python main.py normal` |
| Surto | 8 passageiros chegam quase em simultâneo | `python main.py surge` |

---

## 6. Ficheiros de Log

O ficheiro `airport.log` é gerado automaticamente e contém:
- Hora de chegada de cada passageiro
- Prioridade e classe do bilhete
- Portão e agente alocados
- Tempo de espera na fila
- Duração do embarque
- Desistências com tempo de espera

---

## 7. Decisões de Implementação e Dificuldades

### 7.1 Decisões Tomadas

**Manager vs. shared_memory nativo do Python**  
Optou-se pelo `multiprocessing.Manager` em vez de `multiprocessing.shared_memory` porque o Manager suporta estruturas de dados de alto nível (dict, list, semáforos, locks) de forma transparente entre processos, sem necessidade de serialização manual. A trade-off é um ligeiro overhead de comunicação via socket interno, aceitável para esta escala.

**Threads para embarque, processos para passageiros**  
O servidor usa threads internas para gerir embarques concorrentes (até NUM_GATES em simultâneo), enquanto cada passageiro é um processo. Esta arquitectura híbrida evita o overhead de criar um processo por embarque, mantendo o isolamento entre passageiros.

**Polling leve no passageiro**  
O processo passageiro verifica o seu estado a cada 0.5 segundos em vez de usar uma variável de condição, porque as Condition do Manager têm limitações em cenários multi-processo. O intervalo de 0.5s é um compromisso entre reatividade e carga do sistema.

### 7.2 Dificuldades Encontradas

- **Serialização do Manager.dict**: Ao modificar um dicionário aninhado, é necessário reatribuir o valor completo (`passengers[pid] = p`), pois o Manager não detecta mutações em profundidade.
- **Gestão do ciclo de vida do servidor**: Garantir que o servidor termina apenas após todos os embarques em curso concluírem exigiu um período de espera calculado com base no número de portões e no tempo máximo de embarque.
- **Race condition na desistência**: Um passageiro pode ser retirado da fila pelo thread de vigilância ao mesmo tempo que o servidor tenta processá-lo. Resolvido verificando o estado do passageiro antes de iniciar o embarque.

---

## 8. Propostas de Melhorias Futuras

1. **Interface gráfica em tempo real** – usar `curses` ou `rich` para mostrar a fila e o estado dos portões no terminal com atualização dinâmica.

2. **Persistência em base de dados** – substituir o ficheiro de log por uma base de dados SQLite para consultas mais flexíveis e relatórios históricos.

3. **Configuração dinâmica** – ficheiro de configuração JSON/YAML para ajustar NUM_GATES, NUM_AGENTS, MAX_WAIT e a lista de passageiros sem alterar o código.

4. **Preempção de passageiros** – implementar interrupção de embarques em curso quando chega um passageiro de prioridade muito alta (requer protocolo de comunicação adicional entre servidor e thread de embarque).

5. **Simulação de voos** – agrupar passageiros por voo, com portões dedicados por voo e janelas de embarque com hora de fecho.

6. **Balanceamento de carga entre portões** – alocar o portão com menor fila pendente em vez do primeiro livre, para distribuição mais equilibrada.

7. **Métricas detalhadas por classe** – estatísticas separadas de tempo de espera médio e taxa de desistência para cada classe de bilhete.
