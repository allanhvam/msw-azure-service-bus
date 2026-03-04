import js from "@eslint/js";
import stylistic from "@stylistic/eslint-plugin";
import eslintConfigPrettier from "eslint-config-prettier";
import onlyWarn from "eslint-plugin-only-warn";
import tseslint from "typescript-eslint";

/**
 * A shared ESLint configuration for the repository.
 *
 * @type {import("eslint").Linter.Config}
 * */
export default [
    js.configs.recommended,
    eslintConfigPrettier,
    ...tseslint.configs.recommended,
    {
        plugins: {
            "@stylistic": stylistic,
        },
        rules: {
            "@stylistic/semi": ["error", "always"],
            "@typescript-eslint/consistent-type-imports": "error",
            "@stylistic/comma-dangle": ["error", "always-multiline"],
        },
    },
    {
        plugins: {
            onlyWarn,
        },
    },
    {
        ignores: ["**/dist/**"],
    },
];
