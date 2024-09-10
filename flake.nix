{
  description = "Go flake";

  inputs = { 
    flake-utils.url = "github:numtide/flake-utils"; 
  };

  outputs = { self, flake-utils, nixpkgs }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = nixpkgs.legacyPackages.${system};
          lastModifiedDate = self.lastModifiedDate or self.lastModified or 19700101;
          version = builtins.substring 0 8 lastModifiedDate;
          gopkg = pkgs.buildGoModule {
            pname = "name";
            inherit version;
            src = ./.;
            vendorHash = pkgs.lib.fakeHash;
          };
      in {
        packages = { inherit gopkg; };
        defaultPackage = gopkg;
        devShell = pkgs.mkShell {
          inputsFrom = [ gopkg ];
          buildInputs = [ pkgs.gopls ];
        };
      });
}
