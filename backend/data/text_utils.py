import numpy as np
from cyrtranslit import to_cyrillic
from langchain_google_genai import GoogleGenerativeAIEmbeddings


def normalize_name(name: str) -> str:
    return ' '.join(sorted(to_cyrillic(name.lower(), 'mk').split(' ')))


def get_embeddings_client() -> GoogleGenerativeAIEmbeddings:
    return GoogleGenerativeAIEmbeddings(
        model="models/gemini-embedding-001",
        task_type="semantic_similarity",
        output_dimensionality=768,
    )


def normalize_embedding(embedding: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(embedding)
    if n == 0:
        return embedding
    return embedding / n

