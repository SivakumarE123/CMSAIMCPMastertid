
from presidio_analyzer import PatternRecognizer
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig
from piiservice import analyzer

anonymizer = AnonymizerEngine()


def apply_multiple_deny_lists(text: str, deny_lists: dict):
    """
    Apply multiple deny lists dynamically via API
    """

    recognizers = []

    # Create recognizer per entity
    for entity, values in deny_lists.items():
        recognizers.append(
            PatternRecognizer(
                supported_entity=entity,
                deny_list=values
            )
        )

    # Run analyzer with spaCy + all deny lists
    results = analyzer.analyze(
        text=text,
        language="en",
        ad_hoc_recognizers=recognizers   #  key part
    )

    # Anonymize
    anonymized = anonymizer.anonymize(
        text=text,
        analyzer_results=results,
        operators={
            "DEFAULT": OperatorConfig("replace", {"new_value": "<REDACTED>"})
        }
    )

    return anonymized.text