import tools.text_pipeline
from tools.text_pipeline import gensim_corpora_builder
from tools.text_pipeline import googleanalytics
from tools.text_pipeline import tfidf
from tools.text_pipeline import sklearn_latent_dirichlet_allocation
from tools.text_pipeline import gensim_latent_dirichlet_allocation
from tools.text_pipeline import document_filter
from tools.text_pipeline import sklearn_corpora_builder
from tools.text_pipeline import topic_comparator
from tools.text_pipeline import save_corpus_to_file
from tools.text_pipeline import job_df_analysis
from tools.text_pipeline import df_analysis
from tools.text_sub_pipeline import gensim_lda_grouped
from  tools.text_pipeline import rake_analysis
from tools.text_pipeline import text_cleaner
from tools.text_pipeline import text_rank
from tools.text_sub_pipeline import text_rank_grouped



class PipelineManifest:

    def __init__(self):
        pass

    manifest = {
        "ListOfListsToGensimCorpora" :  tools.text_pipeline.gensim_corpora_builder.ListOfListsToGensimCorpora(),
        "DataframeToListOfLists" :  tools.text_pipeline.gensim_corpora_builder.DataframeToListOfLists(),
        "GensimTfIdfModelBuilder" : tools.text_pipeline.tfidf.GensimTfIdfModelBuilder(),
        "GensimTfIdfTopTerms" : tools.text_pipeline.tfidf.GensimTfIdfTopTerms(),
        "StopWordRemoval" : tools.text_pipeline.document_filter.StopWordRemoval(),
        "UniquePageViewIntegration": tools.text_pipeline.googleanalytics.UniquePageViewIntegration(),
        "UniquePageViewIntegrationDynamic": tools.text_pipeline.googleanalytics.UniquePageViewIntegrationDynamic(),
        "ScikitLDA": tools.text_pipeline.sklearn_latent_dirichlet_allocation.ScikitLDA(),
        "SciKitLDAReport":tools.text_pipeline.sklearn_latent_dirichlet_allocation.SciKitLDAReport(),
        "LinkedDocListToScikitLDACorpus":tools.text_pipeline.sklearn_corpora_builder.LinkedDocListToScikitLDACorpus(),
        "GensimLDA": tools.text_pipeline.gensim_latent_dirichlet_allocation.GensimLDA(),
        "GroupByESIndex": tools.text_pipeline.document_filter.GroupByESIndex(),
        "GensimLdaGrouped_SubPipe": tools.text_sub_pipeline.gensim_lda_grouped.GensimLdaGrouped_SubPipe(),
        "TextRankGrouped_SubPipe": tools.text_sub_pipeline.text_rank_grouped.TextRankGrouped_SubPipe(),
        "GensimTopicSimilarityAnalysis": tools.text_pipeline.gensim_latent_dirichlet_allocation.GensimTopicSimilarityAnalysis(),
        "GensimTopicReduction" : tools.text_pipeline.gensim_latent_dirichlet_allocation.GensimTopicReduction(),
        "TopicLoader" : tools.text_pipeline.topic_comparator.TopicLoader(),
        "TopicComparator" : tools.text_pipeline.topic_comparator.TopicComparator(),
        "SaveDfAsCsv" : tools.text_pipeline.save_corpus_to_file.SaveDfAsCsv(),
        "JobDfAnalysis": tools.text_pipeline.job_df_analysis.JobDfAnalysis(),
        "DfGroupByAnalysis": tools.text_pipeline.df_analysis.DfGroupByAnalysis(),
        "AreasOfStudyDfAnalysis": tools.text_pipeline.job_df_analysis.AreasOfStudyDfAnalysis(),
        "RakeAnalysis": tools.text_pipeline.rake_analysis.RakeAnalysis(),
        "RakeAnalysisFromTextRank": tools.text_pipeline.rake_analysis.RakeAnalysisFromTextRank(),
        "TextCleaner_DF": tools.text_pipeline.text_cleaner.TextCleaner_Df_Corpus(),
        "TextRank": tools.text_pipeline.text_rank.TextRank(),


    }
