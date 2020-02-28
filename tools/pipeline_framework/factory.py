import tools.text_pipeline
from tools.text_pipeline import extract_data
from tools.text_pipeline import gensim_corpora_builder
from tools.text_pipeline import googleanalytics
from tools.text_pipeline import tfidf
from tools.text_pipeline import sklearn_latent_dirichlet_allocation
from tools.text_pipeline import sklearn_random_forest
from tools.text_pipeline import sklearn_utils
from tools.text_pipeline import gensim_latent_dirichlet_allocation
from tools.text_pipeline import glove_trainer
from tools.text_pipeline import document_filter
from tools.text_pipeline import sklearn_corpora_builder
from tools.text_pipeline import sklearn_linear_discriminant_analysis
from tools.text_pipeline import sklearn_kmeans
from tools.text_pipeline import sklearn_principal_component_analysis
from tools.text_pipeline import topic_comparator
from tools.text_pipeline import save_corpus_to_file
from tools.text_pipeline import job_df_analysis
from tools.text_pipeline import df_analysis
from tools.text_pipeline import pipeline_utils
from  tools.text_pipeline import rake_analysis
from tools.text_pipeline import text_cleaner
from tools.text_pipeline import text_rank
from tools.text_pipeline import word_count
from tools.text_pipeline import gensim_word2vec
from tools.text_pipeline import part_of_speech_analyzer
from tools.text_pipeline import analysis_to_linkeddocs_conversion
from tools.text_pipeline import gensim_sentence_finder
from tools.text_pipeline import kmeans_sentence_finder


from tools.text_sub_pipeline import text_rank_grouped
from tools.text_sub_pipeline import text_rank_grouped_simple
from tools.text_sub_pipeline import gensim_lda_grouped
from tools.text_sub_pipeline import stop_word_by_subset
from tools.text_sub_pipeline import kmeans_grouped


class PipelineManifest:

    def __init__(self):
        pass

    manifest = {
        "AreasOfStudyDfAnalysis": tools.text_pipeline.job_df_analysis.AreasOfStudyDfAnalysis(),
        "CountBySpaceAndGroup" : tools.text_pipeline.document_filter.CountBySpaceAndGroup(),
        "CombineSentencesToDocs" : tools.text_pipeline.document_filter.CombineSentencesToDocs(),
        "DataframeToListOfLists" :  tools.text_pipeline.gensim_corpora_builder.DataframeToListOfLists(),
        "DfGroupByAnalysis": tools.text_pipeline.df_analysis.DfGroupByAnalysis(),
        "EsExtract" : tools.text_pipeline.extract_data.EsExtract(),
        "EvenByGroup": tools.text_pipeline.document_filter.EvenByGroup(),
        "EvenBySpace" : tools.text_pipeline.document_filter.EvenBySpace(),
        "ExcludeByGroup": tools.text_pipeline.document_filter.ExcludeByGroup(),
        "ExcludeBySpace": tools.text_pipeline.document_filter.ExcludeBySpace(),
        "FilterTokensByCount" : tools.text_pipeline.document_filter.FilterTokensByCount(),
        "GensimLDA": tools.text_pipeline.gensim_latent_dirichlet_allocation.GensimLDA(),
        "GensimLDADistinctTopics" : tools.text_pipeline.gensim_latent_dirichlet_allocation.GensimLDADistinctTopics(),
        "GensimLdaGrouped_SubPipe": tools.text_sub_pipeline.gensim_lda_grouped.GensimLdaGrouped_SubPipe(),
        "GensimSentenceFinder": tools.text_pipeline.gensim_sentence_finder.GensimSentenceFinder(),
        "GensimTfIdfModelBuilder" : tools.text_pipeline.tfidf.GensimTfIdfModelBuilder(),
        "GensimTfIdfTopTerms" : tools.text_pipeline.tfidf.GensimTfIdfTopTerms(),
        "GensimTopicReduction" : tools.text_pipeline.gensim_latent_dirichlet_allocation.GensimTopicReduction(),
        "GensimTopicSimilarityAnalysis": tools.text_pipeline.gensim_latent_dirichlet_allocation.GensimTopicSimilarityAnalysis(),
        "GensimWord2Vec" : tools.text_pipeline.gensim_word2vec.GensimWord2Vec(),
        "GloveModelBuilder" : tools.text_pipeline.glove_trainer.GloveModelBuilder(),
        "GloveLoadings" : tools.text_pipeline.glove_trainer.GloveLoadings(),
        "JobDfAnalysis": tools.text_pipeline.job_df_analysis.JobDfAnalysis(),
        "KmeansSentenceFinder" : tools.text_pipeline.kmeans_sentence_finder.KmeansSentenceFinder(),
        "KmeansGrouped_SubPipe": tools.text_sub_pipeline.kmeans_grouped.KmeansGrouped_SubPipe(),
        "LemmatizeTokens" : tools.text_pipeline.part_of_speech_analyzer.LemmatizeTokens(),
        "LemmatizeDocs" : tools.text_pipeline.part_of_speech_analyzer.LemmatizeDocs(),
        "Lemmatize_Corpus_LinkedDocs" : tools.text_pipeline.text_cleaner.Lemmatize_Corpus_LinkedDocs(),
        "LinkedDocCorpusStopWordGenerator" : tools.text_pipeline.word_count.LinkedDocCorpusStopWordGenerator(),
        "LinkedDocCorpusWordCount": tools.text_pipeline.word_count.LinkedDocCorpusWordCount(),
        "LinkedDocListToScikitLDACorpus": tools.text_pipeline.sklearn_corpora_builder.LinkedDocListToScikitLDACorpus(),
        "LinkedDocListToScikitRFCorpus" : tools.text_pipeline.sklearn_corpora_builder.LinkedDocListToScikitRFCorpus(),
        "LinkedDocToLinkedSentences" : tools.text_pipeline.document_filter.LinkedDocToLinkedSentences(),
        "ListOfListsToGensimCorpora" :  tools.text_pipeline.gensim_corpora_builder.ListOfListsToGensimCorpora(),
        "Loop" : tools.text_pipeline.pipeline_utils.Loop(),
        "MergeCategories1" : tools.text_pipeline.pipeline_utils.MergeCategories1(),
        "NounPhraseResultsToLinkedDocList": tools.text_pipeline.analysis_to_linkeddocs_conversion.NounPhraseResultsToLinkedDocList(),
        "PartOfSpeechAnalyzerFromTextRank" : tools.text_pipeline.part_of_speech_analyzer.PartOfSpeechAnalyzerFromTextRank(),
        "RakeAnalysis": tools.text_pipeline.rake_analysis.RakeAnalysis(),
        "RakeAnalysisFromTextRank": tools.text_pipeline.rake_analysis.RakeAnalysisFromTextRank(),
        "RakeResultsToLinkedDocList" : tools.text_pipeline.analysis_to_linkeddocs_conversion.RakeResultsToLinkedDocList(),
        "RemoveDuplicateDocs" : tools.text_pipeline.document_filter.RemoveDuplicateDocs(),
        "Reset" : tools.text_pipeline.document_filter.Reset(),
        "SaveDfAsCsv" : tools.text_pipeline.save_corpus_to_file.SaveDfAsCsv(),
        "ScikitAgglomerativeKmeans" : tools.text_pipeline.sklearn_kmeans.ScikitAgglomerativeKmeans(),
        "ScikitKmeansNoRepeats" : tools.text_pipeline.sklearn_kmeans.ScikitKmeansNoRepeats(),
        "ScikitKmeansWithTermRemoval" : tools.text_pipeline.sklearn_kmeans.ScikitKmeansWithTermRemoval(),
        "ScikitLDA": tools.text_pipeline.sklearn_latent_dirichlet_allocation.ScikitLDA(),
        "SciKitLDAReport":tools.text_pipeline.sklearn_latent_dirichlet_allocation.SciKitLDAReport(),
        "ScikitLinearDiscriminantAnalysis" :tools.text_pipeline.sklearn_linear_discriminant_analysis.ScikitLinearDiscriminantAnalysis(),
        "ScikitNearMisses" : tools.text_pipeline.sklearn_utils.ScikitNearMisses(),
        "ScikitKmeansNoRepeats": tools.text_pipeline.sklearn_kmeans.ScikitKmeansNoRepeats(),
        "ScikitPrettyConfusion" : tools.text_pipeline.sklearn_utils.ScikitPrettyConfusion(),
        "ScikitPrincipalComponentAnalysis" : tools.text_pipeline.sklearn_principal_component_analysis.ScikitPrincipalComponentAnalysis(),
        "ScikitRF": tools.text_pipeline.sklearn_random_forest.ScikitRF(),
        "ScikitSentenceFinder" : tools.text_pipeline.sklearn_utils.ScikitSentenceFinder(),
        "StopWord_SubPipe" : tools.text_sub_pipeline.stop_word_by_subset.StopWord_SubPipe(),
        "StopWordRemoval" : tools.text_pipeline.document_filter.StopWordRemoval(),
        "SubsetData": tools.text_pipeline.document_filter.SubsetData(),
        "TextCleaner_DF": tools.text_pipeline.text_cleaner.TextCleaner_Df_Corpus(),
        "TextRank": tools.text_pipeline.text_rank.TextRank(),
        "TextRankGrouped_SubPipe": tools.text_sub_pipeline.text_rank_grouped.TextRankGrouped_SubPipe(),
        "TextRankGroupedSimple_SubPipe": tools.text_sub_pipeline.text_rank_grouped_simple.TextRankGroupedSimple_SubPipe(),
        "TextRankResultsToLinkedDocList" : tools.text_pipeline.analysis_to_linkeddocs_conversion.TextRankResultsToLinkedDocList(),
        "TokensToDoc" : tools.text_pipeline.text_cleaner.TokensToDoc(),
        "TopicComparator" : tools.text_pipeline.topic_comparator.TopicComparator(),
        "TopicLoader" : tools.text_pipeline.topic_comparator.TopicLoader(),
        "UniquePageViewIntegration": tools.text_pipeline.googleanalytics.UniquePageViewIntegration(),
        "UniquePageViewIntegrationDynamic": tools.text_pipeline.googleanalytics.UniquePageViewIntegrationDynamic(),
    }
