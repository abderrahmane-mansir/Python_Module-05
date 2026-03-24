from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):
    def __init__(self) -> None:
        self.processed = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:

        if criteria is None:
            return data_batch

        return [d for d in data_batch if criteria in str(d)]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"processed": self.processed}


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__()
        self.stream_id = stream_id
        self.data_type = "Environmental Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        self.processed += len(data_batch)

        temps = [
            float(x.split(":")[1])
            for x in data_batch
            if "temp" in x
        ]

        avg = sum(temps) / len(temps) if temps else 0

        return (
            f"Sensor analysis: {len(data_batch)} readings processed, "
            f"avg temp: {avg}°C\n"
        )


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__()
        self.stream_id = stream_id
        self.data_type = "Financial Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        self.processed += len(data_batch)

        values = [int(x.split(":")[1]) for x in data_batch]

        net = sum(values[::2]) - sum(values[1::2])

        sign = "+" if net >= 0 else ""

        return (
            f"Transaction analysis: {len(data_batch)} operations, "
            f"net flow: {sign}{net} units\n"
        )


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__()
        self.stream_id = stream_id
        self.data_type = "System Events"

    def process_batch(self, data_batch: List[Any]) -> str:
        self.processed += len(data_batch)

        errors = sum(
            1 for x in data_batch
            if "error" in x
        )

        return (
            f"Event analysis: {len(data_batch)} events, "
            f"{errors} error detected\n"
        )


class StreamProcessor:
    def process_streams(
        self,
        streams: List[DataStream],
        batches: List[List[Any]]
    ) -> None:

        print("Batch 1 Results:")

        critical_sensor = 0
        large_transactions = 0

        for stream, batch in zip(streams, batches):
            stream.process_batch(batch)

            if isinstance(stream, SensorStream):
                print(f"- Sensor data: {len(batch)} readings processed")

                filtered = stream.filter_data(batch, "temp")
                critical_sensor = len(filtered)

            elif isinstance(stream, TransactionStream):
                print(f"- Transaction data: {len(batch)} operations processed")

                filtered = [
                    x for x in batch
                    if "buy" in x and int(x.split(":")[1]) > 15
                ]
                large_transactions = len(filtered)

            elif isinstance(stream, EventStream):
                print(f"- Event data: {len(batch)} events processed")

        print("\nStream filtering active: High-priority data only")
        print(
            f"Filtered results: {critical_sensor} critical sensor alerts, "
            f"{large_transactions} large transaction\n"
        )


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.data_type}")
    sensor_batch = ["temp:22.5", "humidity:65", "pressure:1013"]
    print("Processing sensor batch: ", str(sensor_batch).replace("'", ""))
    print(sensor.process_batch(sensor_batch))

    print("Initializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    print(f"Stream ID: {transaction.stream_id}, Type: {transaction.data_type}")
    transaction_batch = ["buy:100", "sell:150", "buy:75"]
    print("Processing transaction batch: ",
          str(transaction_batch).replace("'", ""))
    print(transaction.process_batch(transaction_batch))

    print("Initializing Event Stream...")
    event = EventStream("EVENT_001")
    print(f"Stream ID: {event.stream_id}, Type: {event.data_type}")
    event_batch = ["login", "error", "logout"]
    print("Processing event batch: ", str(event_batch).replace("'", ""))
    print(event.process_batch(event_batch))

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")

    processor = StreamProcessor()
    streams: List[DataStream] = [sensor, transaction, event]
    batches = [
        ["temp:30", "temp:40"],
        ["buy:15", "sell:5", "buy:20", "sell:5"],
        ["login", "error", "error"]
    ]

    processor.process_streams(streams, batches)

    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
