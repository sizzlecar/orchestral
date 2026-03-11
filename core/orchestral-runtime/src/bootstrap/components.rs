use std::sync::Arc;

use orchestral_core::config::OrchestralConfig;
use orchestral_core::normalizer::PlanNormalizer;
use orchestral_core::recipe::{RecipeRegistry, RECIPE_REGISTRY_COMPONENT_KEY};
use orchestral_core::spi::{ComponentRegistry, SpiError};

use super::BootstrapError;

pub(super) fn extract_recipe_registry_component(
    components: &ComponentRegistry,
) -> Result<Option<Arc<RecipeRegistry>>, BootstrapError> {
    let Some(component) = components.get_named_component(RECIPE_REGISTRY_COMPONENT_KEY) else {
        return Ok(None);
    };
    Arc::downcast::<RecipeRegistry>(component)
        .map(Some)
        .map_err(|_| {
            BootstrapError::Spi(SpiError::Internal(format!(
                "named component '{}' must be Arc<RecipeRegistry>",
                RECIPE_REGISTRY_COMPONENT_KEY
            )))
        })
}

pub(super) fn register_recipe_templates(
    normalizer: &mut PlanNormalizer,
    config: &OrchestralConfig,
    registry: Option<&RecipeRegistry>,
) {
    if let Some(registry) = registry {
        for template in registry.templates() {
            normalizer.register_recipe_template(template.clone());
        }
    }
    for template in &config.recipes.templates {
        normalizer.register_recipe_template(template.clone());
    }
}
