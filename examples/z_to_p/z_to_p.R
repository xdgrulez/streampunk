z_to_p <- function(str) {
    if (str == "z") "p" else if (str == "Z") "P" else str
}

export('z_to_p', z_to_p)
