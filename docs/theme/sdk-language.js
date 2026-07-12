(() => {
	const languages = {
		python: { label: "Python", icon: "python.svg" },
		go: { label: "Go", icon: "go.svg" },
		typescript: { label: "TypeScript", icon: "typescript.svg" },
	};
	let selectedLanguage = "python";
	try {
		const stored = localStorage.getItem("vmon-sdk-language");
		if (stored && languages[stored]) selectedLanguage = stored;
	} catch (_) {}

	const selectLanguage = (language) => {
		selectedLanguage = language;
		try { localStorage.setItem("vmon-sdk-language", language); } catch (_) {}
		for (const group of document.querySelectorAll("[data-sdk-snippets]")) {
			for (const panel of group.querySelectorAll("[data-sdk-language]")) {
				panel.hidden = panel.dataset.sdkLanguage !== language;
			}
			for (const button of group.querySelectorAll(".sdk-language__button")) {
				button.setAttribute("aria-pressed", String(button.dataset.sdkLanguage === language));
			}
		}
	};

	for (const group of document.querySelectorAll("[data-sdk-snippets]")) {
		const available = new Set(
			Array.from(group.querySelectorAll("[data-sdk-language]"), (panel) => panel.dataset.sdkLanguage),
		);
		const controls = document.createElement("div");
		controls.className = "sdk-language";
		controls.setAttribute("role", "group");
		controls.setAttribute("aria-label", "Code example language");

		for (const [language, { label, icon }] of Object.entries(languages)) {
			const button = document.createElement("button");
			button.type = "button";
			button.className = `sdk-language__button sdk-language__button--${language}`;
			button.dataset.sdkLanguage = language;
			button.disabled = !available.has(language);
			button.setAttribute("aria-pressed", String(language === selectedLanguage));

			const image = document.createElement("img");
			image.className = "sdk-language__icon";
			image.src = new URL(`../theme/icons/${icon}`, document.baseURI).href;
			image.alt = "";
			image.setAttribute("aria-hidden", "true");

			const name = document.createElement("span");
			name.textContent = label;
			button.append(image, name);
			button.addEventListener("click", () => selectLanguage(language));
			controls.append(button);
		}
		group.prepend(controls);
	}

	selectLanguage(selectedLanguage);
})();
