from edgarsearch import edgarsearch
search = edgarsearch.Search(
    "20130101",
    "20130110",
    20,
    filter_formtype=["8-K"])
search.download_index()
search.download_filings(
    text_only=True,
    chunk_size=100,
    attempts_max=5,
    timeout=45,
    fname_form="%Y/%m/%Y%m%d_%company_",
    disable_progressbar=True,)
