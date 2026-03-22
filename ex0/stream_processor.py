from typing import Any, List
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        pass


class NumericProcessor(DataProcessor):
    def process(self, data: List[int]) -> str:
        if not self.validate(data):
            raise ValueError("Invalid data for NumericProcessor")
        return f"Processed data: {data}"

    def validate(self, data: Any) -> bool:
        return isinstance(data, list)

    def format_output(self, data: List[int]) -> str:
        if not self.validate(data):
            raise ValueError("Invalid data for NumericProcessor")
        ln = len(data)
        sm = sum(x for x in data)
        ag = sm/ln
        return f"Processed {ln} numeric values, sum={sm}, avg={ag}"


class TextProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError("Invalid data for TextProcessor")
        return f"Processed text data: {data}"

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def format_output(self, data: str) -> str:
        if not self.validate(data):
            raise ValueError("Invalid data for TextProcessor")
        ln = len(data)
        sm = len(data.split())
        return f"Processed text: {ln} characters, {sm} words"


class LogProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError("Invalid data for LogProcessor")
        return f"Processed log data: {data}"

    def validate(self, data: Any) -> bool:
        is_str = isinstance(data, str)
        has_log_level = any(level in data for level in ["ERROR", "WARNING", "INFO"])
        return is_str and has_log_level

    def format_output(self, data: str) -> str:
        if not self.validate(data):
            raise ValueError("Invalid data for LogProcessor")
        type = "ALERT" if "ERROR" in data else "WARNING" if "WARNING" in data else "INFO"
        event = data.split(":")[0]
        msg = data.split(":")[1]
        return f"[{type}] {event} level detected:{msg}"


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    numeric_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()

    try:
        numeric_data = [1, 2, 3, 4, 5]
        print("Initializing Numeric Processor...")
        print(numeric_processor.process(numeric_data))
        print("Validation: Numeric data",
              "verified" if numeric_processor.validate(numeric_data)
              else "not verified")
        print("Output:", numeric_processor.format_output(numeric_data), "\n")
    except Exception as e:
        print(f"An error occurred in Numeric Processor: {e}\n")

    try:
        text_data = "Hello Nexus World"
        print("Initializing Text Processor...")
        print(text_processor.process(text_data))
        print("Validation: Text data",
              "verified" if text_processor.validate(text_data)
              else "not verified")
        print("Output:", text_processor.format_output(text_data), "\n")
    except Exception as e:
        print(f"An error occurred in Text Processor: {e}\n")

    try:
        log_data = "WARNING: system overload detected"
        print("Initializing Log Processor...")
        print(log_processor.process(log_data))
        print("Validation: Log data",
              "verified" if log_processor.validate(log_data)
              else "not verified")
        print("Output:", log_processor.format_output(log_data), "\n")
    except Exception as e:
        print(f"An error occurred in Log Processor: {e}\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
