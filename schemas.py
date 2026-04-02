from pydantic import BaseModel
from typing import Dict,List, Optional

class TextRequest(BaseModel):
    text: str
    model: Optional[str] = "spacy"   # future use

class MultiDenyRequest(BaseModel):
    text: str
    deny_lists: Dict[str, List[str]]
    
class OCRRequest(BaseModel):
    file_base64: str
    mime_type: str
