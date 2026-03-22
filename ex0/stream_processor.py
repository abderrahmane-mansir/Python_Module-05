from typing import Any, List, Dict, Union, Optional
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
        return isinstance(data, (List))

    def format_output(self, data: List[int]) -> str:
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

    def format_output(self, result: str) -> str:
        return f"Formatted output: {result}"


class LogProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError("Invalid data for LogProcessor")
        return f"Processed log data: {data}"

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def format_output(self, result: str) -> str:
        return f"Formatted output: {result}"


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    numeric_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()

    numeric_data = [1, 2, 3, 4, 5]
    text_data = "Hello Nexus World"
    log_data = "ERROR: Connection timeout"

    print("Initializing Numeric Processor...")
    print(numeric_processor.process(numeric_data))
    print("Validation: Numeric data ",
          "verified" if numeric_processor.validate(numeric_data)
          else "not verified")
    print("Output: ", numeric_processor.format_output(numeric_data), "\n")

    print("Initializing Text Processor...")
    print("Processing data: ", text_data)
    print(text_processor)
    print("Initializing Log Processor...")
    print("Processing data: ", log_data)
    print(log_processor)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
