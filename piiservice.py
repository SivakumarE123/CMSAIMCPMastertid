from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig

# Initialize once (important for performance)
analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()


def analyze_and_anonymize(text: str):
    results = analyzer.analyze(text=text, language="en")

    anonymized = anonymizer.anonymize(
        text=text,
        analyzer_results=results,
        operators={
            "DEFAULT": OperatorConfig("replace", {"new_value": "<REDACTED>"})
        }
    )

    return anonymized.text