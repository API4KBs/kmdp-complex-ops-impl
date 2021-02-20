package edu.mayo.kmdp.ops.tranx.bpm;

import edu.mayo.kmdp.knowledgebase.assemblers.rdf.GraphBasedAssembler;
import edu.mayo.kmdp.knowledgebase.constructors.DependencyBasedConstructor;
import java.net.URI;
import org.omg.spec.api4kp._20200801.Answer;
import org.omg.spec.api4kp._20200801.api.inference.v4.server.ReasoningApiInternal._askQuery;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.CompositionalApiInternal._assembleCompositeArtifact;
import org.omg.spec.api4kp._20200801.api.knowledgebase.v4.server.KnowledgeBaseApiInternal._getKnowledgeBaseStructure;
import org.omg.spec.api4kp._20200801.api.repository.asset.v4.KnowledgeAssetCatalogApi;
import org.omg.spec.api4kp._20200801.api.repository.asset.v4.KnowledgeAssetRepositoryApi;
import org.omg.spec.api4kp._20200801.api.terminology.v4.TermsApi;
import org.omg.spec.api4kp._20200801.id.Pointer;
import org.omg.spec.api4kp._20200801.id.ResourceIdentifier;
import org.omg.spec.api4kp._20200801.services.KnowledgeCarrier;

public class PostConstructedCcpmToPlanDefPipeline extends CcpmToPlanDefPipeline {

  _getKnowledgeBaseStructure constructor;
  _assembleCompositeArtifact assembler;

  ResourceIdentifier compositeAssetId;

  public PostConstructedCcpmToPlanDefPipeline(
      KnowledgeAssetCatalogApi cat,
      KnowledgeAssetRepositoryApi repo,
      TermsApi terms,
      _askQuery dataShapeQuery,
      ResourceIdentifier compositeAssetId,
      URI... annotationVocabularies) {
    super(cat, repo, terms, dataShapeQuery, annotationVocabularies);
    this.compositeAssetId = compositeAssetId;
    constructor
        = DependencyBasedConstructor.newInstance(cat, compositeAssetId);
    assembler
        = GraphBasedAssembler.newInstance(repo);
  }


  @Override
  public Answer<Pointer> initKnowledgeBase(KnowledgeCarrier kc) {
    ResourceIdentifier rootId = kc.getAssetId();
    return constructor.getKnowledgeBaseStructure(rootId.getUuid(), rootId.getVersionTag())
        .flatMap(struct -> assembler.assembleCompositeArtifact(struct))
        .flatWhole(kbManager::initKnowledgeBase);
  }


}
