package com.chiwawa.lionheart.api.service.article.articleContent;

import static com.chiwawa.lionheart.common.constant.message.CategoryErrorMessage.*;

import java.util.Comparator;

import com.chiwawa.lionheart.common.exception.ErrorCode;
import com.chiwawa.lionheart.common.exception.model.NotFoundException;
import com.chiwawa.lionheart.common.util.MessageUtils;
import com.chiwawa.lionheart.domain.domain.article.Article;
import com.chiwawa.lionheart.domain.domain.article.articleContent.ArticleContent;
import com.chiwawa.lionheart.domain.domain.article.articleContent.ArticleContentType;
import com.chiwawa.lionheart.domain.domain.article.articleContent.repository.ArticleContentRepository;

public class ArticleContentServiceUtils {

	public static ArticleContent findArticleFirstBodyByArticle(Article article) {
		return article.getArticleContents()
			.stream()
			.filter(a -> a.getType() == ArticleContentType.BODY)
			.sorted(Comparator.comparing(ArticleContent::getOrder))
			.findFirst()
			.orElseThrow(() -> new NotFoundException(
				MessageUtils.generate(NOT_EXIST_ARTICLE_CONTENT_ERROR_MESSAGE, article.getId()),
				ErrorCode.NOT_FOUND_ARTICLE_CONTENT_EXCEPTION));
	}

	public static ArticleContent findArticleEditorNoteContentByArticle(
		ArticleContentRepository articleContentRepository,
		Article article) {
		return articleContentRepository.findArticleEditorNoteContentByArticle(article)
			.orElseThrow(() -> new NotFoundException(
				MessageUtils.generate(NOT_EXIST_ARTICLE_CONTENT_ERROR_MESSAGE, article.getId()),
				ErrorCode.NOT_FOUND_ARTICLE_CONTENT_EXCEPTION));
	}
}
