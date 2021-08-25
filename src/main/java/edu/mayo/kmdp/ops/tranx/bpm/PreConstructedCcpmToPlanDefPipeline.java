package edu.mayo.kmdp.ops.tranx.bpm;

import java.net.URI;
import org.omg.spec.api4kp._20200801.Answer;
import org.omg.spec.api4kp._20200801.api.inference.v4.server.ReasoningApiInternal._askQuery;
import org.omg.spec.api4kp._20200801.api.repository.asset.v4.KnowledgeAssetCatalogApi;
import org.omg.spec.api4kp._20200801.api.repository.asset.v4.KnowledgeAssetRepositoryApi;
import org.omg.spec.api4kp._20200801.api.terminology.v4.TermsApi;
import org.omg.spec.api4kp._20200801.id.Pointer;
import org.omg.spec.api4kp._20200801.id.ResourceIdentifier;
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
    ResourceIdentifier compositeId = kc.getAssetId();
    return repo.getCompositeKnowledgeAssetCarrier(compositeId.getUuid(), compositeId.getVersionTag())
        .flatWhole(m -> kbManager.initKnowledgeBase(m, params));
  }

}
