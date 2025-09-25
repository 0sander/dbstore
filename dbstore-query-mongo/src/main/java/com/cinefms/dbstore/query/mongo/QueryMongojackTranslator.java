package com.cinefms.dbstore.query.mongo;

import com.cinefms.dbstore.query.api.DBStoreQuery;
import com.cinefms.dbstore.query.api.DBStoreQuery.OPERATOR;
import com.cinefms.dbstore.query.api.impl.OrderBy;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QueryMongojackTranslator {

	private static final Log LOGGER = LogFactory.getLog(QueryMongojackTranslator.class);

	public Bson translate(DBStoreQuery in) {
		Bson q = null;
		if (in.getField() != null) {
			switch (in.getComparator()) {
				case EQ:
					q = Filters.eq(in.getField(), in.getValue());
					break;
				case LTE:
					q = Filters.lte(in.getField(), in.getValue());
					break;
				case LT:
					q = Filters.lt(in.getField(), in.getValue());
					break;
				case GTE:
					q = Filters.gte(in.getField(), in.getValue());
					break;
				case GT:
					q = Filters.gt(in.getField(), in.getValue());
					break;
				case NE:
					q = Filters.ne(in.getField(), in.getValue());
					break;
				case ELEM_MATCH:
					q = Filters.elemMatch(in.getField(), translate((DBStoreQuery) in.getValue()));
					break;
				case CONTAINS:
					try {
						q = Filters.regex(in.getField(), Pattern.compile((String) in.getValue(), Pattern.CASE_INSENSITIVE));
					} catch (Exception ex) {
						LOGGER.warn("broken regex '" + in.getValue() + "' ....", ex);
						String x = ((String) in.getValue()).replaceAll("[^\\w\\s]", "");
						q = Filters.regex(in.getField(), Pattern.compile(x, Pattern.CASE_INSENSITIVE));
					}
					break;
				case IN:
					LOGGER.debug(" ##### " + in.getField() + " --- " + in.getValue().getClass());
					q = Filters.in(in.getField(), (Collection<?>) in.getValue());
					break;
				case NIN:
					LOGGER.debug(" ##### " + in.getField() + " --- " + in.getValue().getClass());
					Collection values = in.getValue() != null && in.getValue() instanceof Collection ? (List) in.getValue() : null;
					if (values != null && !values.isEmpty()) {
						q = Filters.nin(in.getField(), values);
					} else {
						q = Filters.exists(in.getField(), false);
					}
					break;
				case EXISTS:
					q = Filters.exists(in.getField(), true);
					break;
				case ALL:
					q = Filters.all(in.getField(), (Collection<?>) in.getValue());
					break;
				default:
					break;
			}
		} else {
			List<DBStoreQuery> n = in.getNested();
			if (n != null && !n.isEmpty()) {
				List<Bson> mq = new ArrayList<>();
				for (DBStoreQuery fq : n) {
					mq.add(translate(fq));
				}
				if (in.getOperator() == OPERATOR.AND) {
					q = Filters.and(mq);
				}
				if (in.getOperator() == OPERATOR.OR) {
					q = Filters.or(mq);
				}
			}
		}
		return q;
	}

	public Bson translateOrderBy(DBStoreQuery query) {
		List<OrderBy> orderBy = query.getOrderBy();

		if (orderBy == null || orderBy.isEmpty()) {
			return null;
		}

		return Sorts.orderBy(
				orderBy.stream()
						.map(it -> it.isAsc() ? Sorts.ascending(it.getField()) : Sorts.descending(it.getField()))
						.collect(Collectors.toList())
		);
	}

}
