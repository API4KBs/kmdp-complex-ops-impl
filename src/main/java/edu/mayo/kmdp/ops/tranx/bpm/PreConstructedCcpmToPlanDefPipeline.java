package edu.mayo.kmdp.ops.tranx.bpm;

import java.net.URI;
import org.omg.spec.api4kp._20200801.Answer;
import org.omg.spec.api4kp._20200801.api.inference.v4.server.ReasoningApiInternal._askQuery;
import org.omg.spec.api4kp._20200801.api.repository.asset.v4.KnowledgeAssetCatalogApi;
import org.omg.spec.api4kp._20200801.api.repository.asset.v4.KnowledgeAssetRepositoryApi;
import org.omg.spec.api4kp._20200801.id.Pointer;
import org.omg.spec.api4kp._20200801.services.KnowledgeCarrier;

public class PreConstructedCcpmToPlanDefPipeline extends CcpmToPlanDefPipeline {

  public PreConstructedCcpmToPlanDefPipeline(
      KnowledgeAssetCatalogApi cat,
      KnowledgeAssetRepositoryApi repo,
      _askQuery dataShapeQuery,
      URI... annotationVocabularies) {
    super(cat, repo, dataShapeQuery, annotationVocabularies);
  }

  @Override
  public Answer<Pointer> initKnowledgeBase(KnowledgeCarrier kc, String params) {
    return kbManager.initKnowledgeBase(kc, params);
  }

}
